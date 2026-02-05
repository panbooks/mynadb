//! WebSocket event stream client

use admin_types::AdminEvent;
use leptos::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};
use js_sys::JsString;

/// WebSocket connection state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsState {
    Connecting,
    Connected,
    Disconnected,
    Error,
}

/// Create a WebSocket connection to the event stream
pub fn create_event_stream(
    on_event: impl Fn(AdminEvent) + 'static,
    on_state_change: impl Fn(WsState) + Clone + 'static,
) -> Result<WebSocket, String> {
    // Get WebSocket URL based on current location
    let window = web_sys::window().ok_or("No window")?;
    let location = window.location();
    let protocol = location.protocol().map_err(|_| "No protocol")?;
    let host = location.host().map_err(|_| "No host")?;

    let ws_protocol = if protocol == "https:" { "wss:" } else { "ws:" };
    let ws_url = format!("{}//{}/admin/api/v1/events/ws", ws_protocol, host);

    // Create WebSocket
    let ws = WebSocket::new(&ws_url).map_err(|e| format!("Failed to create WebSocket: {:?}", e))?;
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    // Clone for closures
    let on_state_change_open = on_state_change.clone();
    let on_state_change_close = on_state_change.clone();
    let on_state_change_error = on_state_change;

    // Set up event handlers
    let onopen_callback = Closure::wrap(Box::new(move |_| {
        on_state_change_open(WsState::Connected);
    }) as Box<dyn FnMut(JsValue)>);
    ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
    onopen_callback.forget();

    let onclose_callback = Closure::wrap(Box::new(move |_: CloseEvent| {
        on_state_change_close(WsState::Disconnected);
    }) as Box<dyn FnMut(CloseEvent)>);
    ws.set_onclose(Some(onclose_callback.as_ref().unchecked_ref()));
    onclose_callback.forget();

    let onerror_callback = Closure::wrap(Box::new(move |_: ErrorEvent| {
        on_state_change_error(WsState::Error);
    }) as Box<dyn FnMut(ErrorEvent)>);
    ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
    onerror_callback.forget();

    let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
        if let Ok(text) = e.data().dyn_into::<JsString>() {
            let text: String = text.into();
            if let Ok(event) = serde_json::from_str::<AdminEvent>(&text) {
                on_event(event);
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
    onmessage_callback.forget();

    Ok(ws)
}

/// Hook to manage WebSocket connection
pub fn use_event_stream() -> (ReadSignal<WsState>, ReadSignal<Vec<AdminEvent>>) {
    let (ws_state, set_ws_state) = create_signal(WsState::Disconnected);
    let (events, set_events) = create_signal(Vec::<AdminEvent>::new());

    // Connect on mount
    create_effect(move |_| {
        set_ws_state.set(WsState::Connecting);

        let _ = create_event_stream(
            move |event| {
                set_events.update(|e| {
                    e.push(event);
                    // Keep only last 100 events
                    if e.len() > 100 {
                        e.remove(0);
                    }
                });
            },
            move |state| {
                set_ws_state.set(state);
            },
        );
    });

    (ws_state, events)
}
