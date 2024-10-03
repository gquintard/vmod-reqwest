#[varnish::vmod(docs = "README.md")]
mod reqwest {
    use crate::reqwest_private::*;
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
            follow: Option<i64>,
            timeout: Option<Duration>,
            connect_timeout: Option<Duration>,
            auto_gzip: Option<bool>,
            auto_deflate: Option<bool>,
            auto_brotli: Option<bool>,
            accept_invalid_certs: Option<bool>,
            accept_invalid_hostnames: Option<bool>,
            http_proxy: Option<&str>,
            https_proxy: Option<&str>,
            probe: Option<Option<Probe>>,
        ) -> Result<Self, VclError> {
            // set some default
            let follow = follow.unwrap_or(10);
            let auto_gzip = auto_gzip.unwrap_or(true);
            let auto_deflate = auto_deflate.unwrap_or(true);
            let auto_brotli = auto_brotli.unwrap_or(true);
            let accept_invalid_certs = accept_invalid_certs.unwrap_or(false);
            let accept_invalid_hostnames = accept_invalid_hostnames.unwrap_or(false);

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
            _ctx: &Ctx,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
            url: &str,
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

        pub fn send(
            &self,
            _ctx: &Ctx,
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
            _ctx: &Ctx,
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
            _ctx: &Ctx,
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
            _ctx: &Ctx,
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
            _ctx: &Ctx,
            #[shared_per_vcl] vp_vcl: Option<&BgThread>,
            #[shared_per_task] vp_task: &mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> Result<Option<String>, Box<dyn Error>> {
            match self.get_resp(vp_vcl, vp_task, name)? {
                Err(e) => Ok(Some(e.to_string())),
                Ok(_) => Ok(None),
            }
        }

        pub fn backend(&self, _ctx: &Ctx) -> VCL_BACKEND {
            self.be.vcl_ptr()
        }
    }

    #[event]
    pub fn event(
        _ctx: &Ctx,
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

mod reqwest_private {
    use anyhow::Error;
    use bytes::Bytes;
    use std::boxed::Box;
    use std::io::Write;
    use std::os::raw::{c_char, c_uint, c_void};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant, SystemTime};

    use ::reqwest::Client;
    use ::reqwest::Url;
    //use reqwest::header::HeaderValue;
    use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
    use varnish::ffi::{BS_CACHED, BS_ERROR, BS_NONE};
    use varnish::vcl::Vsb;
    use varnish::vcl::{log, Ctx, Event, LogTag};
    use varnish::vcl::{Backend, Serve, Transfer /*, VCLBackendPtr*/};
    use varnish::vcl::{Probe, Request as ProbeRequest};
    use varnish::vcl::{VclError, VclResult};

    use varnish::run_vtc_tests;
    run_vtc_tests!("tests/*.vtc");

    pub struct ProbeState {
        spec: Probe,
        history: AtomicU64,
        health_changed: std::time::SystemTime,
        url: Url,
        join_handle: Option<tokio::task::JoinHandle<()>>,
        avg: Mutex<f64>,
    }
    #[allow(non_camel_case_types)]
    pub struct client {
        pub name: String,
        pub be: Backend<VCLBackend, BackendResp>,
    }

    pub struct VCLBackend {
        pub name: String,
        pub bgt: *const BgThread,
        pub client: Client,
        pub probe_state: Option<ProbeState>,
        pub https: bool,
        pub base_url: Option<String>,
    }

    impl<'a> Serve<BackendResp> for VCLBackend {
        fn get_type(&self) -> &str {
            "reqwest"
        }

        fn get_headers(&self, ctx: &mut Ctx<'_>) -> VclResult<Option<BackendResp>> {
            if !self.healthy(ctx).0 {
                return Err("unhealthy".into());
            }

            let bereq = ctx.http_bereq.as_ref().unwrap();

            let bereq_url = bereq.url().unwrap();

            let url = if let Some(base_url) = &self.base_url {
                // if the client has a base_url, prepend it to bereq.url
                format!("{}{}", base_url, bereq_url)
            } else if bereq_url.starts_with('/') {
                // otherwise, if bereq.url looks like a path, try to find a host to build a full URL
                if let Some(host) = bereq.header("host") {
                    format!(
                        "{}://{}{}",
                        if self.https { "https" } else { "http" },
                        host,
                        bereq_url
                    )
                } else {
                    return Err("no host found (reqwest.client doesn't have a base_url, bereq.url doesn't specify a host and bereq.http.host is unset)".into());
                }
            } else {
                // else use bereq.url as-is
                bereq_url.to_string()
            };

            let (req_body_tx, body) = hyper::body::Body::channel();
            let req = Request {
                method: bereq.method().unwrap().to_string(),
                url,
                client: self.client.clone(),
                body: ReqBody::Stream(body),
                vcl: false,
                headers: bereq
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            };

            let mut resp_rx = unsafe { (*self.bgt).spawn_req(req) };

            unsafe {
                struct BodyChan<'a> {
                    chan: hyper::body::Sender,
                    rt: &'a tokio::runtime::Runtime,
                }

                unsafe extern "C" fn body_send_iterate(
                    priv_: *mut c_void,
                    _flush: c_uint,
                    ptr: *const c_void,
                    l: isize,
                ) -> i32 {
                    // nothing to do
                    if ptr.is_null() || l == 0 {
                        return 0;
                    }
                    let body_chan = (priv_ as *mut BodyChan).as_mut().unwrap();
                    let buf = std::slice::from_raw_parts(ptr as *const u8, l as usize);
                    let bytes = hyper::body::Bytes::copy_from_slice(buf);

                    body_chan
                        .rt
                        .block_on(async { body_chan.chan.send_data(bytes).await })
                        .is_err()
                        .into()
                }

                // manually dropped a few lines below
                let bcp = Box::into_raw(Box::new(BodyChan {
                    chan: req_body_tx,
                    rt: &(*self.bgt).rt,
                }));
                let p = bcp as *mut c_void;
                // mimicking V1F_SendReq in varnish-cache
                let bo = ctx.raw.bo.as_mut().unwrap();

                if !bo.bereq_body.is_null() {
                    varnish::ffi::ObjIterate(bo.wrk, bo.bereq_body, p, Some(body_send_iterate), 0);
                } else if !bo.req.is_null() && (*bo.req).req_body_status != BS_NONE.as_ptr() {
                    let i = varnish::ffi::VRB_Iterate(
                        bo.wrk,
                        bo.vsl.as_mut_ptr(),
                        bo.req,
                        Some(body_send_iterate),
                        p,
                    );

                    if (*bo.req).req_body_status != BS_CACHED.as_ptr() {
                        bo.no_retry = "req.body not cached\0".as_ptr() as *const c_char;
                    }

                    if (*bo.req).req_body_status == BS_ERROR.as_ptr() {
                        assert!(i < 0);
                        (*bo.req).doclose = &varnish::ffi::SC_RX_BODY[0];
                    }

                    if i < 0 {
                        return Err("req.body read error".into());
                    }
                }
                // manually drop so reqwest knows there's no more body to push
                drop(Box::from_raw(bcp));
            }
            let resp = match resp_rx.blocking_recv().unwrap() {
                RespMsg::Hdrs(resp) => resp,
                RespMsg::Err(e) => return Err(e.to_string().into()),
                _ => unreachable!(),
            };
            let beresp = ctx.http_beresp.as_mut().unwrap();
            beresp.set_status(resp.status as u16);
            beresp.set_proto("HTTP/1.1")?;
            for (k, v) in &resp.headers {
                beresp.set_header(
                    k.as_str(),
                    v.to_str().map_err(|e| {
                        <std::string::String as Into<VclError>>::into(e.to_string())
                    })?,
                )?;
            }
            Ok(Some(BackendResp {
                bytes: None,
                cursor: 0,
                chan: Some(resp_rx),
                content_length: resp.content_length.map(|s| s as usize),
            }))
        }

        fn event(&self, event: Event) {
            // nothing to do
            let probe_state = match self.probe_state {
                None => return,
                Some(ref probe_state) => probe_state,
            };

            // enter the runtime to
            let _guard = unsafe { (*self.bgt).rt.enter() };
            match event {
                // start the probing loop
                Event::Warm => {
                    spawn_probe(
                        unsafe { &*self.bgt },
                        probe_state as *const ProbeState as *mut ProbeState,
                        self.name.clone(),
                    );
                }
                Event::Cold => {
                    // XXX: we should set the handle to None, be we don't have mutability, oh well...
                    probe_state.join_handle.as_ref().unwrap().abort();
                }
                _ => {}
            }
        }

        fn healthy(&self, _ctx: &mut Ctx<'_>) -> (bool, SystemTime) {
            let probe_state = match self.probe_state {
                None => return (true, SystemTime::UNIX_EPOCH),
                Some(ref ps) => ps,
            };

            assert!(probe_state.spec.window <= 64);

            let bitmap = probe_state.history.load(Ordering::Relaxed);
            (
                is_healthy(bitmap, probe_state.spec.window, probe_state.spec.threshold),
                probe_state.health_changed,
            )
        }

        fn list(&self, ctx: &mut Ctx<'_>, vsb: &mut Vsb<'_>, detailed: bool, json: bool) {
            if self.probe_state.is_none() {
                return self.list_without_probe(ctx, vsb, detailed, json);
            }
            let ProbeState {
                history,
                avg,
                spec: Probe {
                    window, threshold, ..
                },
                ..
            } = self.probe_state.as_ref().unwrap();
            let bitmap = history.load(Ordering::Relaxed);
            let window = *window;
            let threshold = *threshold;
            let health_str = if is_healthy(bitmap, window, threshold) {
                "healthy"
            } else {
                "sick"
            };
            let msg = match (json, detailed) {
                // json, no details
                (true, false) => {
                    format!(
                        "[{}, {}, \"{}\"]",
                        good_probes(bitmap, window),
                        window,
                        health_str,
                    )
                }
                // json, details
                (true, true) => {
                    // TODO: talk to upstream, we shouldn't have to add the colon
                    serde_json::to_string(&self.probe_state.as_ref().unwrap().spec)
                        .as_ref()
                        .unwrap()
                        .to_owned()
                        + ",\n"
                }
                // no json, no details
                (false, false) => {
                    format!("{}/{}\t{}", good_probes(bitmap, window), window, health_str)
                }
                // no json, details
                (false, true) => {
                    let mut s = format!(
                        "
 Current states  good: {:2} threshold: {:2} window: {:2}
  Average response time of good probes: {:.06}
  Oldest ================================================== Newest
  ",
                        good_probes(bitmap, window),
                        threshold,
                        window,
                        avg.lock().unwrap()
                    );
                    for i in 0..64 {
                        s += if bitmap.wrapping_shr(63 - i) & 1 == 1 {
                            "H"
                        } else {
                            "-"
                        };
                    }
                    s
                }
            };
            vsb.cat(&msg).unwrap();
        }
    }

    macro_rules! send {
        ($tx:ident, $payload:expr) => {
            if $tx.send($payload).await.is_err() {
                return;
            }
        };
    }

    pub struct BackendResp {
        pub chan: Option<Receiver<RespMsg>>,
        pub bytes: Option<Bytes>,
        pub cursor: usize,
        pub content_length: Option<usize>,
    }

    impl Transfer for BackendResp {
        fn read(&mut self, mut buf: &mut [u8]) -> VclResult<usize> {
            let mut n = 0;
            loop {
                if self.bytes.is_none() && self.chan.is_some() {
                    match self.chan.as_mut().unwrap().blocking_recv() {
                        Some(RespMsg::Hdrs(_)) => panic!("invalid message type: RespMsg::Hdrs"),
                        Some(RespMsg::Chunk(bytes)) => {
                            self.bytes = Some(bytes);
                            self.cursor = 0
                        }
                        Some(RespMsg::Err(e)) => return Err(e.to_string().into()),
                        None => return Ok(n),
                    };
                }

                let pull_buf = self.bytes.as_ref().unwrap();
                let to_write = &pull_buf[self.cursor..];
                let used = buf.write(to_write).unwrap();
                self.cursor += used;
                n += used;
                assert!(self.cursor <= pull_buf.len());
                if self.cursor == pull_buf.len() {
                    self.bytes = None;
                }
            }
        }

        fn len(&self) -> Option<usize> {
            self.content_length
        }
    }
    #[derive(Debug)]
    pub enum RespMsg {
        Hdrs(Response),
        Chunk(Bytes),
        Err(Error),
    }

    #[derive(Debug)]
    pub struct Entry {
        pub client_name: String,
        pub req_name: String,
        pub transaction: VclTransaction,
    }

    // try to keep the object on stack as small as possible, we'll flesh it out into a reqwest::Request
    // once in the Background thread
    #[derive(Debug)]
    pub struct Request {
        pub url: String,
        pub method: String,
        pub headers: Vec<(String, String)>,
        pub body: ReqBody,
        pub client: Client,
        pub vcl: bool,
    }

    use ::reqwest::header::HeaderMap;

    // calling reqwest::Response::body() consumes the object, so we keep a copy of the interesting bits
    // in this struct
    #[derive(Debug)]
    pub struct Response {
        pub headers: HeaderMap,
        pub content_length: Option<u64>,
        pub body: Option<Bytes>,
        pub status: i64,
    }

    #[derive(Debug)]
    pub enum ReqBody {
        None,
        Full(Vec<u8>),
        Stream(hyper::Body),
    }

    #[derive(Debug)]
    pub enum VclTransaction {
        Transition,
        Req(Request),
        Sent(Receiver<RespMsg>),
        Resp(Result<Response, VclError>),
    }

    impl VclTransaction {
        fn unwrap_resp(&self) -> Result<&Response, VclError> {
            match self {
                VclTransaction::Resp(Ok(rsp)) => Ok(rsp),
                VclTransaction::Resp(Err(e)) => Err(VclError::new(e.to_string())),
                _ => panic!("wrong VclTransaction type"),
            }
        }
        fn into_req(self) -> Request {
            match self {
                VclTransaction::Req(rq) => rq,
                _ => panic!("wrong VclTransaction type"),
            }
        }
    }

    pub struct BgThread {
        pub rt: tokio::runtime::Runtime,
        pub sender: UnboundedSender<(Request, Sender<RespMsg>)>,
    }

    impl BgThread {
        fn spawn_req(&self, req: Request) -> Receiver<RespMsg> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            self.sender.send((req, tx)).unwrap();
            rx
        }
    }

    pub async fn process_req(req: Request, tx: Sender<RespMsg>) {
        let method = match reqwest::Method::from_bytes(req.method.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                send!(tx, RespMsg::Err(e.into()));
                return;
            }
        };
        let mut rreq = req.client.request(method, req.url);
        for (k, v) in req.headers {
            rreq = rreq.header(k, v);
        }
        match req.body {
            ReqBody::None => (),
            ReqBody::Stream(b) => rreq = rreq.body(b),
            ReqBody::Full(v) => rreq = rreq.body(v),
        }
        let mut resp = match rreq.send().await {
            Err(e) => {
                send!(tx, RespMsg::Err(e.into()));
                return;
            }
            Ok(resp) => resp,
        };
        let mut beresp = Response {
            status: resp.status().as_u16() as i64,
            headers: resp.headers().clone(),
            content_length: resp.content_length(),
            body: None,
        };

        if req.vcl {
            beresp.body = match resp.bytes().await {
                Err(e) => {
                    send!(tx, RespMsg::Err(e.into()));
                    return;
                }
                Ok(b) => Some(b),
            };
            send!(tx, RespMsg::Hdrs(beresp));
        } else {
            send!(tx, RespMsg::Hdrs(beresp));

            loop {
                match resp.chunk().await {
                    Ok(None) => return,
                    Ok(Some(bytes)) => {
                        if tx.send(RespMsg::Chunk(bytes)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        send!(tx, RespMsg::Err(e.into()));
                        return;
                    }
                };
            }
        }
    }

    fn good_probes(bitmap: u64, window: u32) -> u32 {
        bitmap.wrapping_shl(64_u32 - window).count_ones()
    }

    fn is_healthy(bitmap: u64, window: u32, threshold: u32) -> bool {
        good_probes(bitmap, window) >= threshold
    }

    fn update_health(
        mut bitmap: u64,
        threshold: u32,
        window: u32,
        probe_ok: bool,
    ) -> (u64, bool, bool) {
        let old_health = is_healthy(bitmap, window, threshold);
        let new_bit = if probe_ok { 1 } else { 0 };
        bitmap = bitmap.wrapping_shl(1) | new_bit;
        let new_health = is_healthy(bitmap, window, threshold);
        (bitmap, new_health, new_health == old_health)
    }

    // cheating hard with the pointer here, but the be_event function will stop us
    // before the references are invalid
    fn spawn_probe(bgt: &'static BgThread, probe_statep: *mut ProbeState, name: String) {
        let probe_state = unsafe { probe_statep.as_mut().unwrap() };
        let spec = probe_state.spec.clone();
        let url = probe_state.url.clone();
        let history = &probe_state.history;
        let avg = &probe_state.avg;
        probe_state.join_handle = Some(bgt.rt.spawn(async move {
            let mut h = 0_u64;
            for i in 0..std::cmp::min(spec.initial, 64) {
                h |= 1 << i;
            }
            history.store(h, Ordering::Relaxed);
            let mut avg_rate = 0_f64;
            loop {
                let msg;
                let mut time = 0_f64;
                let new_bit = match reqwest::ClientBuilder::new()
                    .timeout(spec.timeout)
                    .build()
                    .map(|req| req.get(url.clone()).send())
                {
                    Err(e) => {
                        msg = e.to_string();
                        false
                    }
                    Ok(resp) => {
                        let start = Instant::now();
                        match resp.await {
                            Err(e) => {
                                msg = format!("Error: {}", e);
                                false
                            }
                            Ok(resp) if resp.status().as_u16() as u32 == spec.exp_status => {
                                msg = format!("Success: {}", resp.status().as_u16());
                                if avg_rate < 4.0 {
                                    avg_rate += 1.0;
                                }
                                time = start.elapsed().as_secs_f64();
                                let mut _avg = avg.lock().unwrap();
                                *_avg += (time - *_avg) / avg_rate;
                                true
                            }
                            Ok(resp) => {
                                msg = format!(
                                    "Error: expected {} status, got {}",
                                    spec.exp_status,
                                    resp.status().as_u16()
                                );
                                false
                            }
                        }
                    }
                };
                let bitmap = history.load(Ordering::Relaxed);
                let (bitmap, healthy, changed) =
                    update_health(bitmap, spec.threshold, spec.window, new_bit);
                log(
                    LogTag::BackendHealth,
                    &format!(
                        "{} {} {} {} {} {} {} {} {} {}",
                        name,
                        if changed { "Went" } else { "Still" },
                        if healthy { "healthy" } else { "sick" },
                        "UNIMPLEMENTED",
                        good_probes(bitmap, spec.window),
                        spec.threshold,
                        spec.window,
                        time,
                        *avg.lock().unwrap(),
                        msg
                    ),
                );
                history.store(bitmap, Ordering::Relaxed);
                tokio::time::sleep(spec.interval).await;
            }
        }));
    }

    pub fn build_probe_state(
        mut probe: Probe,
        base_url: Option<&str>,
    ) -> Result<ProbeState, VclError> {
        // sanitize probe (see vbp_set_defaults in Varnish Cache)
        if probe.timeout.is_zero() {
            probe.timeout = Duration::from_secs(2);
        }
        if probe.interval.is_zero() {
            probe.interval = Duration::from_secs(5);
        }
        if probe.window == 0 {
            probe.window = 8;
        }
        if probe.threshold == 0 {
            probe.threshold = 3;
        }
        if probe.exp_status == 0 {
            probe.exp_status = 200;
        }
        if probe.initial == 0 {
            probe.initial = probe.threshold - 1;
        }
        probe.initial = std::cmp::min(probe.initial, probe.threshold);
        let spec_url = match probe.request {
            ProbeRequest::URL(ref u) => u,
            _ => return Err(VclError::new("can't use a probe without .url".to_string())),
        };
        let url = if let Some(base_url) = base_url {
            let full_url = format!("{}{}", base_url, spec_url);
            Url::parse(&full_url).map_err(|e| {
                VclError::new(format!("problem with probe endpoint {full_url} ({e})"))
            })?
        } else if spec_url.starts_with('/') {
            return Err(VclError::new(
                "client has no .base_url, and the probe doesn't have a fully-qualified URL as .url"
                    .to_string(),
            ));
        } else {
            Url::parse(spec_url)
                .map_err(|e| VclError::new(format!("probe endpoint {spec_url} ({e})")))?
        };
        Ok(ProbeState {
            spec: probe,
            history: AtomicU64::new(0),
            health_changed: std::time::SystemTime::now(),
            join_handle: None,
            url,
            avg: Mutex::new(0_f64),
        })
    }

    impl client {
        pub fn vcl_send(&self, bgt: &BgThread, t: &mut VclTransaction) {
            let old_t = std::mem::replace(t, VclTransaction::Transition);
            *t = VclTransaction::Sent(bgt.spawn_req(old_t.into_req()));
        }

        pub fn wait_on(&self, bgt: &BgThread, t: &mut VclTransaction) {
            match t {
                VclTransaction::Req(_) => {
                    self.vcl_send(bgt, t);
                    self.wait_on(bgt, t)
                }
                VclTransaction::Sent(rx) => {
                    *t = match rx.blocking_recv().unwrap() {
                        RespMsg::Hdrs(resp) => VclTransaction::Resp(Ok(resp)),
                        RespMsg::Chunk(_) => unreachable!(),
                        RespMsg::Err(e) => VclTransaction::Resp(Err(VclError::new(e.to_string()))),
                    };
                }
                VclTransaction::Resp(_) => (),
                VclTransaction::Transition => panic!("impossible"),
            }
        }

        pub fn get_transaction<'a>(
            &self,
            vp_task: &'a mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> VclResult<&'a mut VclTransaction> {
            vp_task
                .as_mut()
                .ok_or_else(|| <&str as Into<VclError>>::into(name))?
                .iter_mut()
                .find(|e| name == e.req_name && self.name == e.client_name)
                .map(|e| &mut e.transaction)
                .ok_or_else(|| name.into())
        }

        // we have a stacked Result here because the first one will fail at the
        // vcl level, while the core one is salvageable
        pub fn get_resp<'a>(
            &self,
            vp_vcl: Option<&BgThread>,
            vp_task: &'a mut Option<Box<Vec<Entry>>>,
            name: &str,
        ) -> VclResult<Result<&'a Response, VclError>> {
            let t = self.get_transaction(vp_task, name)?;
            self.wait_on(vp_vcl.as_ref().unwrap(), t);
            Ok(t.unwrap_resp())
        }
    }
}
