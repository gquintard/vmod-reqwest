mod implementation;

use varnish::run_vtc_tests;
run_vtc_tests!("tests/*.vtc");

#[varnish::vmod(docs = "README.md")]
mod reqwest {
    use std::boxed::Box;
    use std::error::Error;
    use std::io::Write;
    use std::time::Duration;

    use reqwest::header::HeaderValue;
    use tokio::sync::mpsc::Sender;
    // FIXME: needed for header()
    use varnish::ffi::{VCL_BACKEND, VCL_STRING};
    use varnish::vcl::{Backend, Ctx, Event, IntoVCL, Probe, VclError};

    use crate::implementation::reqwest_private::*;

    impl client {
        #[allow(clippy::too_many_arguments)]
        /// Create a `client` object that can be used both for backend requests and in-vcl requests and will pool connections across them all. All arguments are optional.
        ///
        /// `base_url` and `https`: dictates how the URL of a backend request is built:
        /// - if `base_url` is specified, the full URL used is `base_url` + `bereq.url`, which means `base_url` nees to specify a scheme (e.g. `http://`) and a host (e.g. `www.example.com).
        /// - otherwise, if `bereq.url`, doesn't start with a `/`, use it as-is
        /// - otherwise, the URL is `http(s)://` + `bereq.http.host` + `bereq.url`, using `https` to decide on the scheme (will fail if there's no bereq.http.host)
        ///
        /// `base_url` and `https` are mutually exclusive and can't be specified together.
        pub fn new(
            ctx: &mut Ctx,
            #[vcl_name] vcl_name: &str,
            #[shared_per_vcl] vp_vcl: &mut Option<Box<BgThread>>,
            base_url: Option<&str>,
            https: Option<bool>,
            /// `follow` dictates whether to follow redirects, and how many hops are allowed before becoming an error. A value of `0` or less will disable redirect folowing,
            /// meaning you will actually receive 30X responses if they are sent by the server.
            #[default(10)]
            follow: i64,
            /// `connect_timeout` and `timeout` dictate how long we give a request to connect, and finish, respectively.
            timeout: Option<Duration>,
            connect_timeout: Option<Duration>,
            /// `auto_gzip`, `auto_deflate` and `auto_brotli`, if set, will automatically set the relevant `accept-encoding` values and automatically decompress the response
            /// body. Note that this will only work if the `accept-encoding` header isn't already set AND if there's no `range` header. In practice, when contacting a backend, you will need to `unset bereq.http.accept-encoding;`, as Varnish sets it automatically.
            #[default(true)]
            auto_gzip: bool,
            #[default(true)] auto_deflate: bool,
            #[default(true)] auto_brotli: bool,
            /// avoid erroring on invalid certificates, for example self-signed ones. It's a dangerous option, use at your own risk!
            #[default(false)]
            accept_invalid_certs: bool,
            /// even more dangerous, doesn't even require for the certificate hostname to match the server being contacted.
            #[default(false)]
            accept_invalid_hostnames: bool,
            /// HTTP proxy to send your requests through
            http_proxy: Option<&str>,
            /// HTTPS proxy to send your requests through
            https_proxy: Option<&str>,
            /// `probe` will work the same way as for regular backends, but there are a few details to be aware of:
            /// - the health will only prevent a fetch for backends (i.e. when using `client.backend()`), not when creating free standing requests (`client.init()`/`client.send()`).
            /// - if the `client` has a `base_url`, the probe will prepend it to its `.url` field to know which URL to probe.
            /// - otherwise, it'll just use the `.url` field as-is (but will immediately error out if `.url` starts with a `/`).
            /// - this means `client`s without`base_url` can actually probe a another server that the one used as a backend.
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

        /// reate an http request, identifying it by its `name`. The request is local to the VCL task it was created in. If a request already existed with the same name, it it simply dropped and replaced, i.e. it is NOT automatically sent.
        pub fn init(
            &self,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// handle for the request, it'll be used by other methods to identify the transaction
            name: &str,
            /// URL/path of the request
            url: &str,
            /// HTTP method to use
            #[default("GET")]
            method: &str,
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

        /// Actually send request `name`. This is non-blocking, and optional if you access the response. Any call to `status()`, `header()`, `body_as_string()` or `error()` will implicitly call `send()` if necessary and wait for the response to arrive.
        ///
        /// `send()` is mainly useful in two cases:
        /// - fire-and-forget: the response won't be checked, but you need the request to be sent away
        /// - early send: you might want to send the request in `vcl_recv` but check the response in `vcl_deliver` to parallelize the VCL processing (backend fetch et al.) with the request.
        pub fn send(
            &self,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
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

        /// Add a new header `name: value` to the unsent request named `name`. Calling this on a non-existing, or already sent request will trigger a VCL error.
        pub fn set_header(
            &self,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
            name: &str,
            /// header name
            key: &str,
            /// header value
            value: &str,
        ) -> Result<(), Box<dyn Error>> {
            if let VclTransaction::Req(req) = self.get_transaction(vp_task, name)? {
                req.headers.push((key.into(), value.into()));
                Ok(())
            } else {
                Err(name.into())
            }
        }

        /// Set the body of the unsent request named `name`. As for `set_header()`, the request must exist and not have been sent.
        pub fn set_body(
            &self,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
            name: &str,
            /// the body to send
            body: &str,
        ) -> Result<(), Box<dyn Error>> {
            if let VclTransaction::Req(req) = self.get_transaction(vp_task, name)? {
                req.body = ReqBody::Full(Vec::from(body));
                Ok(())
            } else {
                Err(name.into())
            }
        }

        /// Copy the native request headers (i.e. `req` or `bereq`) into the request named `name`.
        pub fn copy_headers_to_req(
            &self,
            ctx: &Ctx,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
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

        /// Retrieve the response status (send and wait if necessary), returns 0 if the reponse failed, but will cause a VCL errorif call on a non-existing request.
        pub fn status(
            &self,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
            name: &str,
        ) -> Result<i64, Box<dyn Error>> {
            Ok(self
                .get_resp(vp_vcl, vp_task, name)?
                .map(|r| r.status)
                .unwrap_or(0))
        }

        /// Retrieve the value of the first header named `key`, or returns NULL if it doesn't exist, or there was a transmission error.
        pub unsafe fn header(
            &self,
            ctx: &mut Ctx,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
            name: &str,
            /// header name
            key: &str,
            /// if set, concatenate all headers named `key`, using `sep` as separator
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

        /// Retrieve the response body, returns an empty string in case of error.
        pub unsafe fn body_as_string(
            &self,
            ctx: &mut Ctx,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
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

        /// Returns the error string if request `name` failed.
        pub fn error(
            &self,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            /// request handle
            name: &str,
        ) -> Result<Option<String>, Box<dyn Error>> {
            match self.get_resp(vp_vcl, vp_task, name)? {
                Err(e) => Ok(Some(e.to_string())),
                Ok(_) => Ok(None),
            }
        }

        /// Return a VCL backend built upon the `client` specification
        pub unsafe fn backend(&self) -> VCL_BACKEND {
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
