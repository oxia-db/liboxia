use std::ffi::{c_char, CStr, CString};
use std::slice;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

use liboxia::client::{Client, ClientImpl};
use liboxia::client_options::OxiaClientOptions;
use liboxia::errors::OxiaError;

static GLOBAL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

pub struct OxiaClient(Box<ClientImpl>);

#[repr(C)]
pub struct COxiaClientOptions {
    pub service_address: *const c_char,
    pub namespace: *const c_char,
}

#[repr(C)]
pub struct COxiaPutResult {
    pub key: *mut c_char,
    pub version_id: i64,
}

#[repr(C)]
pub struct COxiaGetResult {
    pub key: *mut c_char,
    pub value: *mut u8,
    pub value_len: usize,
    pub version_id: i64,
}

#[repr(i32)]
#[derive(Debug, PartialEq)]
pub enum COxiaError {
    Ok = 0,
    TransportError = 1,
    GrpcStatus = 2,
    UnexpectedStatus = 3,
    ShardLeaderNotFound = 4,
    KeyLeaderNotFound = 5,
    KeyNotFound = 6,
    UnexpectedVersionId = 7,
    SessionDoesNotExist = 8,
    InternalRetryable = 9,
    Cancelled = 10,
    IllegalArgument = 11,
}

impl From<OxiaError> for COxiaError {
    fn from(error: OxiaError) -> Self {
        match error {
            OxiaError::Transport(_) => COxiaError::TransportError,
            OxiaError::GrpcStatus(_) => COxiaError::GrpcStatus,
            OxiaError::UnexpectedStatus(_) => COxiaError::UnexpectedStatus,
            OxiaError::ShardLeaderNotFound(_) => COxiaError::ShardLeaderNotFound,
            OxiaError::KeyLeaderNotFound(_) => COxiaError::KeyLeaderNotFound,
            OxiaError::KeyNotFound() => COxiaError::KeyNotFound,
            OxiaError::UnexpectedVersionId() => COxiaError::UnexpectedVersionId,
            OxiaError::SessionDoesNotExist() => COxiaError::SessionDoesNotExist,
            OxiaError::InternalRetryable() => COxiaError::InternalRetryable,
            OxiaError::Cancelled() => COxiaError::Cancelled,
            OxiaError::IllegalArgument(_) => COxiaError::IllegalArgument,
        }
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_new(
    options: COxiaClientOptions,
    client_ptr: *mut *mut OxiaClient,
) -> COxiaError {
    let rt = get_runtime();
    let service_address = unsafe {
        CStr::from_ptr(options.service_address)
            .to_str()
            .unwrap()
            .to_string()
    };
    let namespace = unsafe {
        CStr::from_ptr(options.namespace)
            .to_str()
            .unwrap()
            .to_string()
    };

    let native_options = OxiaClientOptions {
        service_address,
        namespace,
        ..Default::default()
    };

    let res = rt.block_on(async { ClientImpl::new(native_options).await });

    match res {
        Ok(client) => {
            unsafe {
                *client_ptr = Box::into_raw(Box::new(OxiaClient(Box::new(client))));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_free(client: *mut OxiaClient) {
    if !client.is_null() {
        unsafe {
            let _ = Box::from_raw(client);
        };
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_put(
    client: *const OxiaClient,
    key: *const c_char,
    value: *const u8,
    value_len: usize,
    result_ptr: *mut *mut COxiaPutResult,
) -> COxiaError {
    let rt = get_runtime();
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap().to_string() };
    let value = unsafe { slice::from_raw_parts(value, value_len).to_vec() };
    let result = rt.block_on(async {
        let rust_client = unsafe { &*client };
        rust_client.0.put_with_options(key, value, vec![]).await
    });

    match result {
        Ok(put_result) => {
            let c_result = COxiaPutResult {
                key: CString::new(put_result.key).unwrap().into_raw(),
                version_id: put_result.version.version_id,
            };
            unsafe {
                *result_ptr = Box::into_raw(Box::new(c_result));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_get(
    client: *const OxiaClient,
    key: *const c_char,
    result_ptr: *mut *mut COxiaGetResult,
) -> COxiaError {
    let rt = get_runtime();
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap().to_string() };
    let result = rt.block_on(async {
        let rust_client = unsafe { &*client };
        rust_client.0.get_with_options(key, vec![]).await
    });

    match result {
        Ok(get_result) => {
            let key_str = CString::new(get_result.key).unwrap();
            let c_result = match get_result.value {
                Some(value) => {
                    let value_ptr = value.as_ptr() as *mut u8;
                    let value_len = value.len();
                    std::mem::forget(value); // Forget vec to prevent it from being freed
                    COxiaGetResult {
                        key: key_str.into_raw(),
                        value: value_ptr,
                        value_len,
                        version_id: get_result.version.version_id,
                    }
                }
                None => COxiaGetResult {
                    key: key_str.into_raw(),
                    value: std::ptr::null_mut(),
                    value_len: 0,
                    version_id: get_result.version.version_id,
                },
            };
            unsafe {
                *result_ptr = Box::into_raw(Box::new(c_result));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_shutdown(client: *mut OxiaClient) -> COxiaError {
    let rt = get_runtime();
    let res = rt.block_on(async {
        let rust_client = unsafe { Box::from_raw(client) };
        rust_client.0.shutdown().await
    });

    match res {
        Ok(_) => COxiaError::Ok,
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_put_result_free(result: *mut COxiaPutResult) {
    if !result.is_null() {
        let box_result = unsafe { Box::from_raw(result) };
        if !box_result.key.is_null() {
            unsafe {
                let _ = CString::from_raw(box_result.key);
            };
        }
    }
}

#[no_mangle]
pub extern "C" fn oxia_get_result_free(result: *mut COxiaGetResult) {
    if !result.is_null() {
        let box_result = unsafe { Box::from_raw(result) };
        if !box_result.key.is_null() {
            unsafe {
                let _ = CString::from_raw(box_result.key);
            };
        }
        if !box_result.value.is_null() {
            unsafe {
                let _ = Vec::from_raw_parts(
                    box_result.value,
                    box_result.value_len,
                    box_result.value_len,
                );
            }
        }
    }
}
