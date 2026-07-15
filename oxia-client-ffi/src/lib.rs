#![allow(clippy::not_unsafe_ptr_arg_deref)]
use std::ffi::{c_char, CStr, CString};
use std::slice;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

use oxia::client::OxiaClient as NativeOxiaClient;
use oxia::client_options::OxiaClientOptions;
use oxia::errors::OxiaError;

static GLOBAL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

pub struct OxiaClient(Box<NativeOxiaClient>);

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
    RequestTimeout = 12,
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
            OxiaError::RequestTimeout() => COxiaError::RequestTimeout,
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

    let res = rt.block_on(async { NativeOxiaClient::new(native_options).await });

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
pub extern "C" fn oxia_client_delete(client: *const OxiaClient, key: *const c_char) -> COxiaError {
    let rt = get_runtime();
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap().to_string() };
    let result = rt.block_on(async {
        let rust_client = unsafe { &*client };
        rust_client.0.delete(key).await
    });

    match result {
        Ok(_) => COxiaError::Ok,
        Err(err) => COxiaError::from(err),
    }
}

#[repr(C)]
pub struct COxiaListResult {
    pub keys: *mut *mut c_char,
    pub keys_len: usize,
}

#[no_mangle]
pub extern "C" fn oxia_client_list(
    client: *const OxiaClient,
    min_key_inclusive: *const c_char,
    max_key_exclusive: *const c_char,
    result_ptr: *mut *mut COxiaListResult,
) -> COxiaError {
    let rt = get_runtime();
    let min_key = unsafe {
        CStr::from_ptr(min_key_inclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let max_key = unsafe {
        CStr::from_ptr(max_key_exclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let result = rt.block_on(async {
        let rust_client = unsafe { &*client };
        rust_client.0.list(min_key, max_key).await
    });

    match result {
        Ok(list_result) => {
            let c_keys: Vec<*mut c_char> = list_result
                .keys
                .into_iter()
                .map(|k| CString::new(k).unwrap().into_raw())
                .collect();
            let keys_len = c_keys.len();
            let keys_ptr = c_keys.as_ptr() as *mut *mut c_char;
            std::mem::forget(c_keys);
            let c_result = COxiaListResult {
                keys: keys_ptr,
                keys_len,
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
pub extern "C" fn oxia_list_result_free(result: *mut COxiaListResult) {
    if !result.is_null() {
        let box_result = unsafe { Box::from_raw(result) };
        if !box_result.keys.is_null() {
            let keys = unsafe {
                Vec::from_raw_parts(box_result.keys, box_result.keys_len, box_result.keys_len)
            };
            for key in keys {
                if !key.is_null() {
                    unsafe {
                        let _ = CString::from_raw(key);
                    }
                }
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_delete_range(
    client: *const OxiaClient,
    min_key_inclusive: *const c_char,
    max_key_exclusive: *const c_char,
) -> COxiaError {
    let rt = get_runtime();
    let min_key = unsafe {
        CStr::from_ptr(min_key_inclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let max_key = unsafe {
        CStr::from_ptr(max_key_exclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let result = rt.block_on(async {
        let rust_client = unsafe { &*client };
        rust_client.0.delete_range(min_key, max_key).await
    });

    match result {
        Ok(_) => COxiaError::Ok,
        Err(err) => COxiaError::from(err),
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
