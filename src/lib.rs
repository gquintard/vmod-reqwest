mod implementation;

use varnish::run_vtc_tests;
run_vtc_tests!("tests/*.vtc");

#[varnish::vmod(docs = "README.md")]
mod reqwest {
    use crate::implementation::reqwest_private::*;
    use std::boxed::Box;
    use std::error::Error;
    use std::io::Write;
    use std::time::Duration;

    use reqwest::header::HeaderValue;
    use tokio::sync::mpsc::Sender;
    use varnish::vcl::Backend;
    use varnish::vcl::Probe;
    use varnish::vcl::{Ctx, Event};

    // FIXME: needed for header()
    use varnish::ffi::{VCL_BACKEND, VCL_STRING};
    use varnish::vcl::{IntoVCL, VclError};

    impl client {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            ctx: &mut Ctx,
            #[vcl_name] vcl_name: &str,
            #[shared_per_vcl] vp_vcl: &mut Option<Box<BgThread>>,
            base_url: Option<&str>,
            https: Option<bool>,
            #[arg(default = 10)] follow: i64,
            timeout: Option<Duration>,
            connect_timeout: Option<Duration>,
            #[arg(default = true)] auto_gzip: bool,
            #[arg(default = true)] auto_deflate: bool,
            #[arg(default = true)] auto_brotli: bool,
            #[arg(default = false)] accept_invalid_certs: bool,
            #[arg(default = false)] accept_invalid_hostnames: bool,
            http_proxy: Option<&str>,
            https_proxy: Option<&str>,
            probe: Option<Probe>,
        ) -> Result<Self, VclError> {
            // set some default
            let mut rcb = reqwest::ClientBuilder::new()
                .brotli(auto_brotli)
                .deflate(auto_deflate)
                .gzip(auto_gzip)
                .danger_accept_invalid_certs(accept_invalid_certs)
                .danger_accept_invalid_hostnames(accept_invalid_hostnames);
            if let Some(t) = timeout {
                rcb = rcb.timeout(t);
            }
            if let Some(t) = connect_timeout {
                rcb = rcb.connect_timeout(t);
            }
            if let Some(proxy) = http_proxy {
                rcb = rcb.proxy(reqwest::Proxy::https(proxy).map_err(|e| {
                    VclError::new(format!(
                        "reqwest: couldn't initialize {vcl_name}'s HTTP proxy ({e})"
                    ))
                })?);
            }
            if let Some(proxy) = https_proxy {
                rcb = rcb.proxy(reqwest::Proxy::https(proxy).map_err(|e| {
                    VclError::new(format!(
                        "reqwest: couldn't initialize {vcl_name}'s HTTPS proxy ({e})"
                    ))
                })?);
            }
            if follow <= 0 {
                rcb = rcb.redirect(reqwest::redirect::Policy::none());
            } else {
                rcb = rcb.redirect(reqwest::redirect::Policy::limited(follow as usize));
            }
            let reqwest_client = rcb.build().map_err(|e| {
                VclError::new(format!("reqwest: couldn't initialize {vcl_name} ({e})"))
            })?;

            if https.is_some() && base_url.is_some() {
                return Err(VclError::new(format!("reqwest: couldn't initialize {vcl_name}: can't take both an https and a base_url argument")));
            }

            let probe_state = match probe {
                Some(spec) => Some(build_probe_state(spec, base_url).map_err(|e| {
                    VclError::new(format!("reqwest: failed to add probe to {vcl_name} ({e})"))
                })?),
                None => None,
            };
            let has_probe = probe_state.is_some();

            let be = Backend::new(
                ctx,
                vcl_name,
                VCLBackend {
                    name: vcl_name.to_string(),
                    bgt: &**vp_vcl.as_ref().unwrap(),
                    client: reqwest_client,
                    probe_state,
                    https: https.unwrap_or(false),
                    base_url: base_url.map(|s| s.into()),
                },
                has_probe,
            )?;
            let client = client {
                name: vcl_name.to_owned(),
                be,
            };
            Ok(client)
        }

        pub fn init(
            &self,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
            url: &str,
            #[arg(default = "GET")] method: &str,
        ) {
            if vp_task.as_ref().is_none() {
                *vp_task = Some(Box::new(Vec::new()));
            }

            let ts = vp_task.as_mut().unwrap();
            let t = VclTransaction::Req(Request {
                method: method.into(),
                url: url.into(),
                headers: Vec::new(),
                body: ReqBody::None,
                client: self.be.get_inner().client.clone(),
                vcl: true,
            });

            match ts
                .iter_mut()
                .find(|e| e.req_name == name && e.client_name == self.name)
            {
                None => ts.push(Entry {
                    transaction: t,
                    req_name: name.to_owned(),
                    client_name: self.name.to_owned(),
                }),
                Some(e) => e.transaction = t,
            }
        }

        pub fn send(
            &self,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> Result<(), Box<dyn Error>> {
            let t = self.get_transaction(vp_task, name)?;

            if matches!(t, VclTransaction::Req(_)) {
                self.vcl_send(vp_vcl.as_ref().unwrap(), t);
                Ok(())
            } else {
                Err(name.into())
            }
        }

        pub fn set_header(
            &self,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
            key: &str,
            value: &str,
        ) -> Result<(), Box<dyn Error>> {
            if let VclTransaction::Req(req) = self.get_transaction(vp_task, name)? {
                req.headers.push((key.into(), value.into()));
                Ok(())
            } else {
                Err(name.into())
            }
        }

        pub fn set_body(
            &self,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
            body: &str,
        ) -> Result<(), Box<dyn Error>> {
            if let VclTransaction::Req(req) = self.get_transaction(vp_task, name)? {
                req.body = ReqBody::Full(Vec::from(body));
                Ok(())
            } else {
                Err(name.into())
            }
        }

        pub fn copy_headers_to_req(
            &self,
            ctx: &Ctx,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> Result<(), Box<dyn Error>> {
            let req = match self.get_transaction(vp_task, name)? {
                VclTransaction::Req(req) => req,
                _ => return Err(name.into()),
            };
            // XXX: we'll always have one of those, but maybe people would want
            // `req_top`, or even `bereq` while in `vcl_pipe`?
            let vcl_req = ctx.http_req.as_ref().or(ctx.http_bereq.as_ref()).unwrap();

            for hdr in vcl_req {
                req.headers.push((hdr.0.into(), hdr.1.into()));
            }

            Ok(())
        }

        pub fn status(
            &self,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> Result<i64, Box<dyn Error>> {
            Ok(self
                .get_resp(vp_vcl, vp_task, name)?
                .map(|r| r.status)
                .unwrap_or(0))
        }

        pub fn header(
            &self,
            ctx: &mut Ctx,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
            key: &str,
            sep: Option<&str>,
        ) -> Result<VCL_STRING, VclError> {
            // get the number of headers matching, and an iterator of them
            let (n, mut all_headers) = match self.get_resp(vp_vcl, vp_task, name)? {
                Err(_) => return Ok(VCL_STRING::default()),
                Ok(resp) => {
                    let keys = resp.headers.get_all(key);
                    (keys.iter().count(), keys.iter())
                }
            };

            match (n, sep) {
                (0, _) => Ok(VCL_STRING::default()),
                (_, None) => all_headers
                    .next()
                    .map(HeaderValue::as_ref)
                    .into_vcl(&mut ctx.ws),
                (_, Some(s)) => {
                    let mut ws = ctx.ws.reserve();
                    for (i, h) in all_headers.enumerate() {
                        if i != 0 {
                            ws.buf
                                .write(s.as_ref())
                                .map_err(|e| VclError::new(e.to_string()))?;
                        }

                        ws.buf
                            .write(h.as_ref())
                            .map_err(|e| VclError::new(e.to_string()))?;
                    }
                    let buf = ws.release(0);
                    buf.into_vcl(&mut ctx.ws)
                }
            }
        }

        pub fn body_as_string(
            &self,
            ctx: &mut Ctx,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> Result<VCL_STRING, VclError> {
            let body = self
                .get_resp(vp_vcl, vp_task, name)
                .map_err(|e| VclError::new(e.to_string()))?;
            match body {
                Err(_) => Ok(VCL_STRING::default()),
                Ok(resp) => match resp.body {
                    None => Ok(VCL_STRING::default()),
                    Some(ref b) => b.into_vcl(&mut ctx.ws),
                },
            }
        }

        pub fn error(
            &self,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> Result<Option<String>, Box<dyn Error>> {
            match self.get_resp(vp_vcl, vp_task, name)? {
                Err(e) => Ok(Some(e.to_string())),
                Ok(_) => Ok(None),
            }
        }

        pub fn backend(&self) -> VCL_BACKEND {
            self.be.vcl_ptr()
        }
    }

    #[event]
    pub fn event(
        #[shared_per_vcl] vp_vcl: &mut Option<Box<BgThread>>,
        event: Event,
    ) -> Result<(), Box<dyn Error>> {
        // we only need to worry about Load, BgThread will be destroyed with the VPriv when the VCL is
        // discarded
        if let Event::Load = event {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (sender, mut receiver) =
                tokio::sync::mpsc::unbounded_channel::<(Request, Sender<RespMsg>)>();
            rt.spawn(async move {
                loop {
                    let (req, tx) = receiver.recv().await.unwrap();
                    tokio::spawn(async move {
                        process_req(req, tx).await;
                    });
                }
            });
            *vp_vcl = Some(Box::new(BgThread { rt, sender }));
        }
        Ok(())
    }
}
