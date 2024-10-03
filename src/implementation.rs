pub mod reqwest_private {
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
