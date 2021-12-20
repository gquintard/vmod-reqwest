varnish::boilerplate!();

use bytes::Bytes;
use varnish::vcl::ctx::{Ctx, Event, LogTag, TestCtx};
use varnish::vcl::vpriv::VPriv;

pub struct Entry {
    name: String,
    transaction: Transaction,
}

pub enum Transaction {
    Transition,
    Req(reqwest::RequestBuilder),
    Sent(tokio::sync::mpsc::Receiver<Result<Response, String>>),
    Resp(Result<Response, String>),
}

// calling reqwest::Response::body() consumes the object, so we keep a copy of the interesting bits
// in this struct
pub struct Response {
    headers: reqwest::header::HeaderMap,
    body: Option<Bytes>,
    status: i64,
}

impl Transaction {
    fn unwrap_resp(&self) -> &Result<Response, String> {
        match self {
            Transaction::Resp(ref rsp) => rsp,
            _ => panic!("wrong Transaction type"),
        }
    }
    fn into_req(self) -> reqwest::RequestBuilder {
        match self {
            Transaction::Req(rq) => rq,
            _ => panic!("wrong Transaction type"),
        }
    }
}

varnish::vtc!(test01);
pub fn init(
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
    let t = Transaction::Req(vp_vcl.as_ref().unwrap().client.request(
        reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?,
        url,
    ));

    match ts.into_iter().find(|e| e.name == name) {
        None => ts.push(Entry {
            transaction: t,
            name: name.to_owned(),
        }),
        Some(e) => e.transaction = t,
    }
    Ok(())
}

fn _send(bgt: &BgThread, t: &mut Transaction) {
    let sender = bgt.sender.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let old_t = std::mem::replace(t, Transaction::Sent(rx));

    let _ = sender.send((old_t.into_req(), tx));
}

fn wait_on(bgt: &BgThread, t: &mut Transaction) -> Result<(), String> {
    match t {
        Transaction::Req(_) => {
            _send(bgt, t);
            wait_on(bgt, t)
        }
        Transaction::Sent(rx) => {
            *t = Transaction::Resp(rx.blocking_recv().unwrap());
            Ok(())
        }
        Transaction::Resp(_) => Ok(()),
        Transaction::Transition => panic!("impossible"),
    }
}

fn get_transaction<'a, 'b>(
    vp_task: &'a mut VPriv<Vec<Entry>>,
    name: &'b str,
) -> Result<&'a mut Transaction, String> {
    vp_task
        .as_mut()
        .ok_or(format!("reqwest: request \"{}\" isn't initialized", name))?
        .iter_mut()
        .find(|e| name == e.name)
        .map(|e| &mut e.transaction)
        .ok_or(format!("reqwest: request \"{}\" isn't initialized", name))
}

pub fn send(
    _ctx: &Ctx,
    vp_vcl: &mut VPriv<BgThread>,
    vp_task: &mut VPriv<Vec<Entry>>,
    name: &str,
) -> Result<(), String> {
    let t = get_transaction(vp_task, name)?;

    match t {
        Transaction::Req(_) => {
            _send(vp_vcl.as_ref().unwrap(), t);
            Ok(())
        }
        _ => Err(format!("reqwest: request \"{}\" isn't initialized", name)),
    }
}

pub fn exists(_ctx: &Ctx, vp_task: &mut VPriv<Vec<Entry>>, name: &str) -> bool {
    get_transaction(vp_task, name).is_ok()
}

pub fn set_header(
    _ctx: &Ctx,
    vp_task: &mut VPriv<Vec<Entry>>,
    name: &str,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let t = get_transaction(vp_task, name)?;
    if let Transaction::Req(_) = t {
        let old_t = std::mem::replace(t, Transaction::Transition);
        *t = Transaction::Req(old_t.into_req().header(key, value));
        Ok(())
    } else {
        Err(format!("reqwest: request \"{}\" isn't initialized", name))
    }
}

pub fn set_body(
    _ctx: &Ctx,
    vp_task: &mut VPriv<Vec<Entry>>,
    name: &str,
    body: &str,
) -> Result<(), String> {
    let t = get_transaction(vp_task, name)?;
    if let Transaction::Req(_) = t {
        let old_t = std::mem::replace(t, Transaction::Transition);
        // it's a bit annoying to have to copy the body, but the request may outlive our task
        *t = Transaction::Req(old_t.into_req().body(body.to_owned()));
        Ok(())
    } else {
        Err(format!("reqwest: request \"{}\" isn't initialized", name))
    }
}

pub fn status(
    _ctx: &Ctx,
    vp_vcl: &mut VPriv<BgThread>,
    vp_task: &mut VPriv<Vec<Entry>>,
    name: &str,
) -> Result<i64, String> {
    let t = get_transaction(vp_task, name)?;
    wait_on(vp_vcl.as_ref().unwrap(), t)?;

    Ok(t.unwrap_resp().as_ref().map(|rsp| rsp.status).unwrap_or(0))
}

pub fn header<'a>(
    _ctx: &Ctx,
    vp_vcl: &mut VPriv<BgThread>,
    vp_task: &'a mut VPriv<Vec<Entry>>,
    name: &str,
    key: &str,
) -> Result<Option<&'a [u8]>, String> {
    let t = get_transaction(vp_task, name)?;
    wait_on(vp_vcl.as_ref().unwrap(), t)?;

    Ok(t.unwrap_resp()
        .as_ref()
        .ok()
        .map(|rsp| rsp.headers.get(key).map(|h| h.as_ref()))
        .unwrap_or(None))
}

pub fn body_as_string<'a>(
    _ctx: &Ctx,
    vp_vcl: &mut VPriv<BgThread>,
    vp_task: &'a mut VPriv<Vec<Entry>>,
    name: &str,
) -> Result<&'a [u8], String> {
    let t = get_transaction(vp_task, name)?;
    wait_on(vp_vcl.as_ref().unwrap(), t)?;

    match t.unwrap_resp() {
        Err(_) => Ok("".as_ref()),
        Ok(rsp) => Ok(rsp.body.as_ref().unwrap()),
    }
}

pub fn error(
    _ctx: &Ctx,
    vp_vcl: &mut VPriv<BgThread>,
    vp_task: &mut VPriv<Vec<Entry>>,
    name: &str,
) -> Result<Option<String>, String> {
    let t = get_transaction(vp_task, name)?;
    wait_on(vp_vcl.as_ref().unwrap(), t)?;

    match t.unwrap_resp() {
        Err(e) => Ok(Some(e.to_string())),
        Ok(_) => Ok(None),
    }
}

pub struct BgThread {
    sender: tokio::sync::mpsc::UnboundedSender<(
        reqwest::RequestBuilder,
        tokio::sync::mpsc::Sender<Result<Response, String>>,
    )>,
    #[allow(dead_code)]
    rt: tokio::runtime::Runtime,
    client: reqwest::Client,
}

pub unsafe fn event(
    _ctx: &Ctx,
    vp: &mut VPriv<BgThread>,
    event: Event,
) -> Result<(), &'static str> {
    match event {
        Event::Load => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<(
                reqwest::RequestBuilder,
                tokio::sync::mpsc::Sender<Result<Response, String>>,
            )>();
            let client = reqwest::Client::builder().build().unwrap();
            rt.spawn(async move {
                loop {
                    let (req, tx) = receiver.recv().await.unwrap();
                    tokio::spawn(async move {
                        let metadata = match req.send().await {
                            Err(e) => {
                                let _ = tx.send(Err(e.to_string())).await;
                                return;
                            }
                            Ok(rsp) => rsp,
                        };
                        println!("-------------- {:?}", metadata);
                        let mut resp = Response {
                            headers: metadata.headers().clone(),
                            status: metadata.status().as_u16() as i64,
                            body: None,
                        };

                        resp.body = match metadata.bytes().await {
                            Err(e) => {
                                let _ = tx.send(Err(e.to_string())).await;
                                return;
                            }
                            Ok(b) => Some(b),
                        };
                        // we don't care about the result, somebody may receive our message, or
                        // not, we don't care
                        let _ = tx.send(Ok(resp)).await;
                    });
                }
            });
            vp.store(BgThread { sender, rt, client });
        }
        _ => (),
    }
    Ok(())
}
