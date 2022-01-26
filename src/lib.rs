varnish::boilerplate!();

use bytes::Bytes;
use std::boxed::Box;
use std::io::Write;
use std::os::raw::{c_uint, c_void};
use std::ptr;
use std::time::Duration;
use varnish::vcl::ctx::{Ctx, Event};
use varnish::vcl::http::HTTP;
use varnish::vcl::vpriv::VPriv;

use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use varnish::vcl::processor::{PullResult, VFPCtx, VFP};

varnish::vtc!(test01);
varnish::vtc!(test02);
varnish::vtc!(test03);
varnish::vtc!(test04);
varnish::vtc!(test05);
varnish::vtc!(test06);

#[allow(non_camel_case_types)]
#[derive(Debug, Clone)]
struct client {
    name: String,
    client: reqwest::Client,
    be: *const varnish_sys::director,
    https: bool,
    base_url: Option<String>,
}

struct VCLBackend {
    bgt: *const BgThread,
    client: client,
}

const METHODS: *const varnish_sys::vdi_methods = &varnish_sys::vdi_methods {
    magic: varnish_sys::VDI_METHODS_MAGIC,
    type_: "test_be\0".as_ptr() as *const std::os::raw::c_char,
    gethdrs: Some(gethdrs),
    finish: Some(finish),
    destroy: None,
    event: None,
    getip: None,
    healthy: None,
    http1pipe: None,
    list: None,
    panic: None,
    resolve: None,
} as *const varnish_sys::vdi_methods;

impl client {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: &mut Ctx,
        vcl_name: &str,
        vp_vcl: &mut VPriv<BgThread>,
        base_url: Option<&str>,
        https: Option<bool>,
        follow: Option<i64>,
        timeout: Option<Duration>,
        connect_timeout: Option<Duration>,
        auto_gzip: bool,
        auto_deflate: bool,
        auto_brotli: bool,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
        http_proxy: Option<&str>,
        https_proxy: Option<&str>,
    ) -> Self {
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
            match reqwest::Proxy::http(proxy) {
                Ok(p) => {
                    rcb = rcb.proxy(p);
                }
                Err(e) => {
                    ctx.fail(&e.to_string());
                }
            }
        }
        if let Some(proxy) = https_proxy {
            match reqwest::Proxy::http(proxy) {
                Ok(p) => {
                    rcb = rcb.proxy(p);
                }
                Err(e) => {
                    ctx.fail(&e.to_string());
                }
            }
        }
        if let Some(limit) = follow {
            if limit <= 0 {
                rcb = rcb.redirect(reqwest::redirect::Policy::none());
            } else {
                rcb = rcb.redirect(reqwest::redirect::Policy::limited(limit as usize));
            }
        }
        let reqwest_client = match rcb.build() {
            Ok(rc) => rc,
            Err(e) => {
                ctx.fail(&e.to_string());
                reqwest::Client::new()
            }
        };
        if https.is_some() && base_url.is_some() {
            ctx.fail("reqwest: client() can't take both an https and a base_url argument");
        }
        let mut client = client {
            name: vcl_name.to_owned(),
            client: reqwest_client,
            https: https.unwrap_or(false),
            be: std::ptr::null(),
            base_url: base_url.map(|s| s.into()),
        };

        let backend_p = Box::into_raw(Box::new(VCLBackend {
            bgt: vp_vcl.as_ref().unwrap() as *const BgThread,
            // annoying: we need to clone the whole string because we don't have a stable address
            // for it, oh well...
            client: client.clone(),
        })) as *mut VCLBackend;
        client.be = unsafe {
            varnish_sys::VRT_AddDirector(
                ctx.raw,
                METHODS,
                backend_p as *mut std::ffi::c_void,
                format!("reqwest_backend_{}\0", vcl_name).as_ptr() as *const i8,
            )
        };
        assert!(!client.be.is_null());
        client
    }

    pub fn init(
        &mut self,
        _ctx: &Ctx,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
        method: &str,
        url: &str,
    ) -> Result<(), String> {
        if vp_task.as_ref().is_none() {
            vp_task.store(Vec::new());
        }

        let ts = vp_task.as_mut().unwrap();
        let t = VclTransaction::Req(Request {
            method: method.into(),
            url: url.into(),
            headers: Vec::new(),
            body: Body::None,
            client: self.client.clone(),
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
        Ok(())
    }

    fn vcl_send(&self, bgt: &BgThread, t: &mut VclTransaction) {
        let old_t = std::mem::replace(t, VclTransaction::Transition);
        *t = VclTransaction::Sent(bgt.spawn_req(old_t.into_req()));
    }

    fn wait_on(&self, bgt: &BgThread, t: &mut VclTransaction) {
        match t {
            VclTransaction::Req(_) => {
                self.vcl_send(bgt, t);
                self.wait_on(bgt, t)
            }
            VclTransaction::Sent(rx) => {
                *t = match rx.blocking_recv().unwrap() {
                    RespMsg::Hdrs(resp) => VclTransaction::Resp(Ok(resp)),
                    RespMsg::Chunk(_) => unreachable!(),
                    RespMsg::Err(e) => VclTransaction::Resp(Err(e)),
                };
            }
            VclTransaction::Resp(_) => (),
            VclTransaction::Transition => panic!("impossible"),
        }
    }

    fn get_transaction<'a, 'b>(
        &self,
        vp_task: &'a mut VPriv<Vec<Entry>>,
        name: &'b str,
    ) -> Result<&'a mut VclTransaction, String> {
        vp_task
            .as_mut()
            .ok_or(format!("reqwest: request \"{}\" isn't initialized", name))?
            .iter_mut()
            .find(|e| name == e.req_name && self.name == e.client_name)
            .map(|e| &mut e.transaction)
            .ok_or(format!("reqwest: request \"{}\" isn't initialized", name))
    }

    pub fn send(
        &mut self,
        _ctx: &Ctx,
        vp_vcl: &mut VPriv<BgThread>,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
    ) -> Result<(), String> {
        let t = self.get_transaction(vp_task, name)?;

        match t {
            VclTransaction::Req(_) => {
                self.vcl_send(vp_vcl.as_ref().unwrap(), t);
                Ok(())
            }
            _ => Err(format!("reqwest: request \"{}\" isn't initialized", name)),
        }
    }

    pub fn exists(&mut self, _ctx: &Ctx, vp_task: &mut VPriv<Vec<Entry>>, name: &str) -> bool {
        self.get_transaction(vp_task, name).is_ok()
    }

    pub fn set_header(
        &mut self,
        _ctx: &Ctx,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
        key: &str,
        value: &str,
    ) -> Result<(), String> {
        match self.get_transaction(vp_task, name)? {
            VclTransaction::Req(req) => {
                req.headers.push((key.into(), value.into()));
                Ok(())
            }
            _ => Err(format!("reqwest: request \"{}\" isn't initialized", name)),
        }
    }

    pub fn set_body(
        &mut self,
        _ctx: &Ctx,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
        body: &str,
    ) -> Result<(), String> {
        match self.get_transaction(vp_task, name)? {
            VclTransaction::Req(req) => {
                req.body = Body::Full(Vec::from(body));
                Ok(())
            }
            _ => Err(format!("reqwest: request \"{}\" isn't initialized", name)),
        }
    }

    pub fn status(
        &mut self,
        _ctx: &Ctx,
        vp_vcl: &mut VPriv<BgThread>,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
    ) -> Result<i64, String> {
        let t = self.get_transaction(vp_task, name)?;
        self.wait_on(vp_vcl.as_ref().unwrap(), t);

        Ok(t.unwrap_resp().as_ref().map(|rsp| rsp.status).unwrap_or(0))
    }

    pub fn header<'a>(
        &mut self,
        _ctx: &Ctx,
        vp_vcl: &mut VPriv<BgThread>,
        vp_task: &'a mut VPriv<Vec<Entry>>,
        name: &str,
        key: &str,
    ) -> Result<Option<&'a [u8]>, String> {
        let t = self.get_transaction(vp_task, name)?;
        self.wait_on(vp_vcl.as_ref().unwrap(), t);

        Ok(t.unwrap_resp()
            .as_ref()
            .ok()
            .map(|rsp| rsp.headers.get(key).map(|h| h.as_ref()))
            .unwrap_or(None))
    }

    pub fn body_as_string<'a>(
        &mut self,
        _ctx: &Ctx,
        vp_vcl: &mut VPriv<BgThread>,
        vp_task: &'a mut VPriv<Vec<Entry>>,
        name: &str,
    ) -> Result<&'a [u8], String> {
        let t = self.get_transaction(vp_task, name)?;
        self.wait_on(vp_vcl.as_ref().unwrap(), t);

        match t.unwrap_resp() {
            Err(_) => Ok("".as_ref()),
            Ok(rsp) => Ok(rsp.body.as_ref().unwrap()),
        }
    }

    pub fn error(
        &mut self,
        _ctx: &Ctx,
        vp_vcl: &mut VPriv<BgThread>,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
    ) -> Result<Option<String>, String> {
        let t = self.get_transaction(vp_task, name)?;
        self.wait_on(vp_vcl.as_ref().unwrap(), t);

        match t.unwrap_resp() {
            Err(e) => Ok(Some(e.to_string())),
            Ok(_) => Ok(None),
        }
    }

    pub fn backend(&self, _ctx: &Ctx) -> *const varnish_sys::director {
        self.be
    }
}

#[derive(Debug)]
enum RespMsg {
    Hdrs(Response),
    Chunk(Bytes),
    Err(String),
}

#[derive(Debug)]
pub struct Entry {
    client_name: String,
    req_name: String,
    transaction: VclTransaction,
}

#[derive(Debug)]
enum VclTransaction {
    Transition,
    Req(Request),
    Sent(Receiver<RespMsg>),
    Resp(Result<Response, String>),
}

// try to keep the object on stack as small as possible, we'll flesh it out into a reqwest::Request
// once in the Background thread
#[derive(Debug)]
struct Request {
    url: String,
    method: String,
    headers: Vec<(String, String)>,
    body: Body,
    client: reqwest::Client,
    vcl: bool,
}

// calling reqwest::Response::body() consumes the object, so we keep a copy of the interesting bits
// in this struct
#[derive(Debug)]
pub struct Response {
    headers: reqwest::header::HeaderMap,
    body: Option<Bytes>,
    status: i64,
}

#[derive(Debug)]
enum Body {
    None,
    Full(Vec<u8>),
    Stream(hyper::Body),
}

impl VclTransaction {
    fn unwrap_resp(&self) -> &Result<Response, String> {
        match self {
            VclTransaction::Resp(ref rsp) => rsp,
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
    rt: tokio::runtime::Runtime,
    sender: UnboundedSender<(Request, Sender<RespMsg>)>,
}

impl BgThread {
    fn spawn_req(&self, req: Request) -> Receiver<RespMsg> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.sender.send((req, tx)).unwrap();
        rx
    }
}

async fn process_req(req: Request, tx: Sender<RespMsg>) {
    let method = match reqwest::Method::from_bytes(req.method.as_bytes()) {
        Ok(m) => m,
        Err(e) => {
            tx.send(RespMsg::Err(e.to_string())).await.unwrap();
            return;
        }
    };
    let mut rreq = req.client.request(method, req.url);
    for (k, v) in req.headers {
        rreq = rreq.header(k, v);
    }
    match req.body {
        Body::None => (),
        Body::Stream(b) => rreq = rreq.body(b),
        Body::Full(v) => rreq = rreq.body(v),
    }
    let mut resp = match rreq.send().await {
        Err(e) => {
            tx.send(RespMsg::Err(e.to_string())).await.unwrap();
            return;
        }
        Ok(resp) => resp,
    };
    let mut beresp = Response {
        status: resp.status().as_u16() as i64,
        headers: resp.headers().clone(),
        body: None,
    };

    if req.vcl {
        beresp.body = match resp.bytes().await {
            Err(e) => {
                tx.send(RespMsg::Err(e.to_string())).await.unwrap();
                return;
            }
            Ok(b) => Some(b),
        };
        tx.send(RespMsg::Hdrs(beresp)).await.unwrap();
    } else {
        tx.send(RespMsg::Hdrs(beresp)).await.unwrap();

        loop {
            match resp.chunk().await {
                Ok(None) => return,
                Ok(Some(bytes)) => {
                    if tx.send(RespMsg::Chunk(bytes)).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(RespMsg::Err(e.to_string())).await.unwrap();
                    return;
                }
            };
        }
    }
}

struct BackendResp {
    chan: Option<Receiver<RespMsg>>,
    bytes: Option<Bytes>,
    cursor: usize,
}

impl VFP for BackendResp {
    fn pull(&mut self, _ctx: &mut VFPCtx, mut buf: &mut [u8]) -> PullResult {
        let mut n = 0;
        loop {
            if buf.is_empty() {
                return PullResult::Ok(n);
            }

            if self.bytes.is_none() && self.chan.is_some() {
                match self.chan.as_mut().unwrap().blocking_recv() {
                    Some(RespMsg::Hdrs(_)) => panic!("invalid message type: RespMsg::Hdrs"),
                    Some(RespMsg::Chunk(bytes)) => {
                        self.bytes = Some(bytes);
                        self.cursor = 0
                    }
                    Some(RespMsg::Err(_)) => return PullResult::Err,
                    None => return PullResult::End(n),
                };
            }

            let to_write = &self.bytes.as_ref().unwrap()[self.cursor..];
            let _n = buf.write(to_write).unwrap();
            self.cursor += _n;
            n += _n;
            assert!(self.cursor <= self.bytes.as_ref().unwrap().len());
            if self.cursor == self.bytes.as_ref().unwrap().len() {
                self.bytes = None;
            }
        }
    }
}

unsafe impl Sync for VfpWrapper {}
struct VfpWrapper {
    vfp: varnish_sys::vfp,
}

static REQWEST_VFP: VfpWrapper = VfpWrapper {
    vfp: varnish_sys::vfp {
        name: "reqwest\0".as_ptr() as *const i8,
        init: None,
        pull: Some(varnish::vcl::processor::wrap_vfp_pull::<BackendResp>),
        fini: Some(varnish::vcl::processor::wrap_vfp_fini::<BackendResp>),
        priv1: ptr::null(),
    },
};

struct BodyChan<'a> {
    chan: hyper::body::Sender,
    rt: &'a tokio::runtime::Runtime,
}

unsafe extern "C" fn body_send_iterate(
    priv_: *mut c_void,
    _flush: c_uint,
    ptr: *const c_void,
    l: varnish_sys::ssize_t,
) -> i32 {
    let body_chan = (priv_ as *mut BodyChan).as_mut().unwrap();
    let buf = std::slice::from_raw_parts(ptr as *const u8, l as usize);
    let bytes = hyper::body::Bytes::copy_from_slice(buf);

    let err = body_chan
        .rt
        .block_on(async { body_chan.chan.send_data(bytes).await })
        .is_err();
    if err {
        1
    } else {
        0
    }
}

unsafe extern "C" fn gethdrs(
    ctx: *const varnish_sys::vrt_ctx,
    be: varnish_sys::VCL_BACKEND,
) -> ::std::os::raw::c_int {
    let VCLBackend { bgt: bgtp, client } = ((*be).priv_ as *const VCLBackend).as_ref().unwrap();
    let bgt = bgtp.as_ref().unwrap();
    let bereq = HTTP::new((*(*ctx).bo).bereq).unwrap();
    let url = match (&client.base_url, client.https) {
        (Some(url), _) => String::new() + url + bereq.url().unwrap(),
        (None, true) => {
            String::new() + "https://" + bereq.header("host").unwrap() + bereq.url().unwrap()
        }
        (None, false) => {
            String::new() + "http://" + bereq.header("host").unwrap() + bereq.url().unwrap()
        }
    };

    let (req_body_tx, body) = hyper::body::Body::channel();
    let req = Request {
        method: bereq.method().unwrap().to_string(),
        url,
        client: client.client.clone(),
        body: Body::Stream(body),
        vcl: false,
        headers: bereq
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
    };

    let mut resp_rx = bgt.spawn_req(req);

    let bcp = Box::into_raw(Box::new(BodyChan {
        chan: req_body_tx,
        rt: &bgt.rt,
    }));
    let p = bcp as *mut c_void;
    // mimicking V1F_SendReq in varnish-cache
    let bo = (*ctx).bo.as_mut().unwrap();
    if !(*bo).bereq_body.is_null() {
        varnish_sys::ObjIterate(bo.wrk, bo.bereq_body, p, Some(body_send_iterate), 0);
    } else if !bo.req.is_null() && (*bo.req).req_body_status != varnish_sys::BS_NONE.as_ptr() {
        let i = varnish_sys::VRB_Iterate(
            bo.wrk,
            bo.vsl.as_mut_ptr(),
            bo.req,
            Some(body_send_iterate),
            p,
        );

        if (*bo.req).req_body_status != varnish_sys::BS_CACHED.as_ptr() {
            bo.no_retry = "req.body not cached\0".as_ptr() as *const i8;
        }

        if (*bo.req).req_body_status == varnish_sys::BS_ERROR.as_ptr() {
            assert!(i < 0);
            (*bo.req).doclose = varnish_sys::sess_close_SC_RX_BODY;
        }
    }
    drop(Box::from_raw(bcp));

    let resp = match resp_rx.blocking_recv().unwrap() {
        RespMsg::Hdrs(resp) => resp,
        RespMsg::Err(_) => return 1,
        _ => unreachable!(),
    };
    let mut beresp = HTTP::new((*(*ctx).bo).beresp).unwrap();
    beresp.set_status(resp.status as u16);
    beresp.set_proto("HTTP/1.1").unwrap();
    for (k, v) in &resp.headers {
        beresp.set_header(k.as_str(), v.to_str().unwrap()).unwrap();
    }
    (*(*ctx).bo).htc = varnish_sys::WS_Alloc(
        (*(*ctx).bo).ws.as_mut_ptr(),
        std::mem::size_of::<varnish_sys::http_conn>() as u32,
    ) as *mut varnish_sys::http_conn;
    let htc = (*(*ctx).bo).htc.as_mut().unwrap(); // TODO: check ws return
    htc.magic = varnish_sys::HTTP_CONN_MAGIC;
    htc.body_status = varnish_sys::BS_CHUNKED.as_ptr();

    let vfe = varnish_sys::VFP_Push((*(*ctx).bo).vfc, &REQWEST_VFP.vfp);
    if vfe.is_null() {
        1 // TODO better err code
    } else {
        let brm = BackendResp {
            bytes: None,
            cursor: 0,
            chan: Some(resp_rx),
        };
        let respp = Box::into_raw(Box::new(brm));
        (*vfe).priv1 = respp as *mut std::ffi::c_void;
        0
    }
}

unsafe extern "C" fn finish(ctx: *const varnish_sys::vrt_ctx, _arg1: varnish_sys::VCL_BACKEND) {
    (*(*ctx).bo).htc = ptr::null_mut();
}

pub(crate) unsafe fn event(
    _ctx: &Ctx,
    vp: &mut VPriv<BgThread>,
    event: Event,
) -> Result<(), &'static str> {
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
        vp.store(BgThread { rt, sender });
    }
    Ok(())
}
