varnish::boilerplate!();

use bytes::Bytes;
use std::boxed::Box;
use std::io::Write;
use std::os::raw::{c_uint, c_void};
use std::ptr;
use varnish::vcl::ctx::{Ctx, Event};
use varnish::vcl::http::HTTP;
use varnish::vcl::vpriv::VPriv;

use tokio::sync::mpsc::Receiver;
use varnish::vcl::processor::{PullResult, VFPCtx, VFP};

varnish::vtc!(test01);
varnish::vtc!(test02);
varnish::vtc!(test03);
varnish::vtc!(test04);

#[allow(non_camel_case_types)]
struct client {
    name: String,
    reqwest_client: reqwest::Client,
}

impl client {
    pub fn new(_ctx: &Ctx, vcl_name: &str) -> Self {
        client{
            name: vcl_name.to_owned(),
            reqwest_client: reqwest::Client::new(),
        }
    }
    pub fn init(
        &mut self,
        _ctx: &Ctx,
        vp_vcl: &mut VPriv<BgThread>,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
        method: &str,
        url: &str,
        ) -> Result<(), String> {
        if vp_task.as_ref().is_none() {
            vp_task.store(Vec::new());
        }

        let ts = vp_task.as_mut().unwrap();
        let t = VclTransaction::Req(vp_vcl.as_ref().unwrap().client.request(
                reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?,
                url,
                ));

        match ts.iter_mut().find(|e| e.req_name == name && e.client_name == self.name) {
            None => ts.push(Entry {
                transaction: t,
                req_name: name.to_owned(),
                client_name: self.name.to_owned(),
            }),
            Some(e) => e.transaction = t,
        }
        Ok(())
    }

    fn vcl_send(bgt: &BgThread, t: &mut VclTransaction) {
        let old_t = std::mem::replace(t, VclTransaction::Transition);
        *t = VclTransaction::Sent(bgt.spawn_req(old_t.into_req(), true));
    }

    fn wait_on(bgt: &BgThread, t: &mut VclTransaction) {
        match t {
            VclTransaction::Req(_) => {
                client::vcl_send(bgt, t);
                client::wait_on(bgt, t)
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
                client::vcl_send(vp_vcl.as_ref().unwrap(), t);
                Ok(())
            }
            _ => Err(format!("reqwest: request \"{}\" isn't initialized", name)),
        }
    }

    pub fn exists(
        &mut self,
        ctx: &Ctx, vp_task: &mut VPriv<Vec<Entry>>, name: &str) -> bool {
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
        let t = self.get_transaction(vp_task, name)?;
        if let VclTransaction::Req(_) = t {
            let old_t = std::mem::replace(t, VclTransaction::Transition);
            *t = VclTransaction::Req(old_t.into_req().header(key, value));
            Ok(())
        } else {
            Err(format!("reqwest: request \"{}\" isn't initialized", name))
        }
    }

    pub fn set_body(
        &mut self,
        _ctx: &Ctx,
        vp_task: &mut VPriv<Vec<Entry>>,
        name: &str,
        body: &str,
        ) -> Result<(), String> {
        let t = self.get_transaction(vp_task, name)?;
        if let VclTransaction::Req(_) = t {
            let old_t = std::mem::replace(t, VclTransaction::Transition);
            // it's a bit annoying to have to copy the body, but the request may outlive our task
            *t = VclTransaction::Req(old_t.into_req().body(body.to_owned()));
            Ok(())
        } else {
            Err(format!("reqwest: request \"{}\" isn't initialized", name))
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
        client::wait_on(vp_vcl.as_ref().unwrap(), t);

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
        client::wait_on(vp_vcl.as_ref().unwrap(), t);

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
        client::wait_on(vp_vcl.as_ref().unwrap(), t);

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
        client::wait_on(vp_vcl.as_ref().unwrap(), t);

        match t.unwrap_resp() {
            Err(e) => Ok(Some(e.to_string())),
            Ok(_) => Ok(None),
        }
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
    Req(reqwest::RequestBuilder),
    Sent(Receiver<RespMsg>),
    Resp(Result<Response, String>),
}

// calling reqwest::Response::body() consumes the object, so we keep a copy of the interesting bits
// in this struct
#[derive(Debug)]
pub struct Response {
    headers: reqwest::header::HeaderMap,
    body: Option<Bytes>,
    status: i64,
}

impl VclTransaction {
    fn unwrap_resp(&self) -> &Result<Response, String> {
        match self {
            VclTransaction::Resp(ref rsp) => rsp,
            _ => panic!("wrong VclTransaction type"),
        }
    }
    fn into_req(self) -> reqwest::RequestBuilder {
        match self {
            VclTransaction::Req(rq) => rq,
            _ => panic!("wrong VclTransaction type"),
        }
    }
}

pub struct BgThread {
    rt: tokio::runtime::Runtime,
    client: reqwest::Client,
    backend: Option<*const varnish_sys::director>,
}

impl BgThread {
    fn spawn_req(&self, req: reqwest::RequestBuilder, is_vcl: bool) -> Receiver<RespMsg> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.rt.spawn(async move {
            let mut resp = match req.send().await {
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

            if is_vcl {
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
        });
        rx
    }
}

pub fn be(_: &Ctx, vp: &mut VPriv<BgThread>) -> *const varnish_sys::director {
    vp.as_ref().unwrap().backend.unwrap()
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
    let (req_body_tx, body) = hyper::body::Body::channel();
    let bereq = HTTP::new((*(*ctx).bo).bereq).unwrap();
    let client = reqwest::Client::new();
    let mut req = client
        .request(
            reqwest::Method::from_bytes(bereq.method().unwrap().as_bytes()).unwrap(),
            String::new() + "http://" + bereq.header("host").unwrap() + bereq.url().unwrap(),
        )
        .body(body);
    for (k, v) in &bereq {
        req = req.header(k, v);
    }

    let bgt = ((*be).priv_ as *const BgThread).as_ref().unwrap();
    let mut resp_rx = bgt.spawn_req(req, false);

    let bcp = Box::into_raw(Box::new(BodyChan {
        chan: req_body_tx,
        rt: &bgt.rt,
    }));
    let p = bcp as *mut c_void;
    // mimicking V1F_SendReq in varnish-cache
    let bo = (*ctx).bo.as_mut().unwrap();
    if !(*bo).bereq_body.is_null() {
        dbg!();
        varnish_sys::ObjIterate(bo.wrk, bo.bereq_body, p, Some(body_send_iterate), 0);
    } else if !bo.req.is_null() && (*bo.req).req_body_status != varnish_sys::BS_NONE.as_ptr() {
        dbg!();
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

#[allow(clippy::missing_safety_doc)]
pub unsafe fn event(ctx: &Ctx, vp: &mut VPriv<BgThread>, event: Event) -> Result<(), &'static str> {
    match event {
        Event::Load => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let client = reqwest::Client::builder().build().unwrap();
            vp.store(BgThread {
                rt,
                client,
                backend: None,
            });
        }
        Event::Warm => {
            let methods = Box::new(varnish_sys::vdi_methods {
                magic: varnish_sys::VDI_METHODS_MAGIC,
                type_: "test_be\0".as_ptr() as *const std::os::raw::c_char,
                gethdrs: Some(gethdrs),
                finish: Some(finish),
                ..std::default::Default::default()
            });
            let mut bgt = vp.as_mut().unwrap();
            let be = varnish_sys::VRT_AddDirector(
                ctx.raw,
                &*methods,
                bgt as *const BgThread as *mut std::ffi::c_void,
                "test_be\0".as_ptr() as *const i8,
            );
            assert!(!be.is_null());
            bgt.backend = Some(be);
        }
        Event::Cold => {
            let bgt = vp.as_mut().unwrap();
            varnish_sys::VRT_DelDirector(&mut bgt.backend.unwrap());
        }
        _ => (),
    }
    Ok(())
}
