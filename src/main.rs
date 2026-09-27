use std::{collections::HashMap, io::sink, net::SocketAddr, sync::Arc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{net::{TcpListener, TcpStream}, sync::{broadcast}};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};

use pipewire as pw;

mod utils;

use utils::commands::volume;
use utils::types;
use std::sync::Mutex;
use crate::utils::commands::{pipewire_client::{PipeWireEvent, run_pipewire_thread}, volume::{Application, Node, VolumeCommand}};

#[derive(Clone)]
struct DeviceLists {
    application_list: Arc<Mutex<HashMap<u32, volume::Application>>>,
    sink_list: Arc<Mutex<HashMap<u32, volume::Node>>>,
    source_list: Arc<Mutex<HashMap<u32, volume::Node>>>,
    default_source: Arc<Mutex<String>>,
    default_sink: Arc<Mutex<String>>
}


#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080";


    // channel to send data from pipewire thread back to this one to send as websocket msg
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PipeWireEvent>();
    let (bcast_tx, _) = tokio::sync::broadcast::channel::<PipeWireEvent>(100);

    // channel to send events into the pipewire thread
    // (pw nodes only live inside the thread so in order to update them (valume, ...) we need to send events in to the thread)
    let (pw_in_tx, pw_out_rx) = pw::channel::channel::<VolumeCommand>();
    
    let application_list: Arc<Mutex<HashMap<u32, volume::Application>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let sink_list: Arc<Mutex<HashMap<u32, volume::Node>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let source_list: Arc<Mutex<HashMap<u32, volume::Node>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let default_sink: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let default_source: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));

    let application_list_for_pw = application_list.clone();
    let sink_list_for_pw = sink_list.clone();
    let source_list_for_pw = source_list.clone();
    let def_sink_for_pw = default_sink.clone();
    let def_source_for_pw = default_source.clone();

    let devices = Arc::new(DeviceLists { application_list: application_list, sink_list: sink_list, source_list: source_list, default_source, default_sink });

    std::thread::spawn(move || {
        if let Err(e) = run_pipewire_thread(tx, pw_out_rx, DeviceLists { application_list: application_list_for_pw, sink_list: sink_list_for_pw, source_list: source_list_for_pw, default_sink: def_sink_for_pw, default_source: def_source_for_pw}) {
            eprintln!("pipewire thread error: {e}");
        }
    });

    // 2. Bridge task: drain the mpsc queue, fan out via broadcast.
    let bcast_tx2 = bcast_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let _ = bcast_tx2.send(event); // ignore error if no subscribers yet
        }
    });



    let listener = TcpListener::bind(addr).await.expect("Failed to bind");
    println!("WebSocket server listening on ws://{}", addr);

    while let Ok((stream, peer_addr)) = listener.accept().await {
        println!("New connection from: {}", peer_addr);
        let rx: broadcast::Receiver<PipeWireEvent> = bcast_tx.subscribe();
        let pw_in_tx = pw_in_tx.clone();

        tokio::spawn(handle_connection(stream, peer_addr, rx, pw_in_tx, devices.clone()));
    }
}

async fn handle_connection(stream: TcpStream, peer_addr: SocketAddr, mut rx: broadcast::Receiver<PipeWireEvent>, pw_in_tx: pw::channel::Sender<VolumeCommand>, devices: Arc<DeviceLists>) {
    let ws_stream = accept_async(stream)
        .await
        .expect("Error during WebSocket handshake");

    println!("WebSocket handshake complete with: {}", peer_addr);

    let (mut write, mut read) = ws_stream.split();

    let init_data = json!({
        "type": "InitData",
        "payload": {
            "default_sink": &devices.default_sink.lock().unwrap().clone(), 
            "default_source": &devices.default_sink.lock().unwrap().clone(),
            "sinks": &devices.sink_list.lock().unwrap().values().cloned().collect::<Vec<volume::Node>>(),
            "sources": &devices.source_list.lock().unwrap().values().cloned().collect::<Vec<volume::Node>>(),
            "applications": &devices.application_list.lock().unwrap().values().cloned().collect::<Vec<Application>>(),
        }
    });
    if write.send(Message::Text(init_data.to_string().into())).await.is_err() {
        println!("Failed to send initial data to: {}", peer_addr);
    }

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let json = serde_json::to_string(&event).unwrap();
                        if write.send(Message::Text(json.into())).await.is_err() {
                            println!("Failed to send volume event to: {}", peer_addr);
                            break;
                        }

                        
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("Client {} lagged, missed {} events", peer_addr, n);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {

                        match serde_json::from_str::<types::Message>(&text).unwrap() {
                            types::Message::Volume(v) => {
                                let v_clone = v.clone();
                                let _ = pw_in_tx.send(v);
                                // volume::handle_volume(v_clone)
                            },
                            types::Message::Other(o) => {
                                println!("other: {}", o.data)
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        println!("Connection closed by: {}", peer_addr);
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        eprintln!("Error reading from {}: {}", peer_addr, e);
                        break;
                    }
                    None => break, // stream ended
                }
            }
        }
    }

    println!("Connection terminated: {}", peer_addr);
}
