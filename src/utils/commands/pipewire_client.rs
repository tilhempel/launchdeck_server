use pipewire::{keys, node::NodeListener, types::ObjectType::{self}};
use tokio::sync::mpsc::UnboundedSender;
use pipewire as pw;
use pw::spa::param::ParamType;
use pw::spa::pod::deserialize::PodDeserializer;
use pw::spa::pod::Value;
use std::{cell::RefCell, sync::mpsc::Receiver};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use tokio::{net::{TcpListener, TcpStream}, sync::{broadcast}};
use std::sync::Mutex;



use crate::{DeviceLists, utils::commands::volume::{Application, CombType, VolumeCommand::{self, ToggleMuteSink}}};
use crate::volume::Device;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum PipeWireEvent {
    Volume(VolumeEvent),
    DefaultDevice(DefaultDeviceEvent)
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct VolumeEvent {
    pub class: String,
    pub emitter: CombType
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DefaultDeviceEvent {
    pub id: u32,
    pub name: String,
    pub class: String  // "Sind or Source"
}

#[derive(serde::Deserialize)]
struct DefaultNodeValue {
    name: String,
}


pub fn run_pipewire_thread(tx: UnboundedSender<PipeWireEvent>, pw_in_rx: pw::channel::Receiver<VolumeCommand>, device_lists: DeviceLists) -> Result<(), pw::Error> {
    pw::init();

    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;
    let registry = core.get_registry_rc()?;

    let nodes: Rc<RefCell<HashMap<u32, (pw::node::Node, pw::node::NodeListener)>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let metadata_storage: Rc<RefCell<Option<(pw::metadata::Metadata, pw::metadata::MetadataListener)>>> =
        Rc::new(RefCell::new(None));

    let registry_clone = registry.clone();
    let nodes_clone = nodes.clone();
    let metadata_storage_clone = metadata_storage.clone();

    let application_list_for_pw_clone = device_lists.application_list.clone();
    let application_list_for_pw_clone_remove = Arc::clone(&device_lists.application_list);
    let sink_list_for_pw_clone = Arc::clone(&device_lists.sink_list);
    let sink_list_for_pw_clone_remove = Arc::clone(&device_lists.sink_list);
    let source_list_for_pw_clone = Arc::clone(&device_lists.source_list);
    let source_list_for_pw_clone_remove = Arc::clone(&device_lists.source_list);


    let _reg_listener = registry
        .add_listener_local()
        .global(move |global| {
            match global.type_ {
                // default device updates
                ObjectType::Metadata => {
                    let Some(props) = &global.props else { return };
                    if props.get("metadata.name")  != Some("default") {
                        return
                    }

                    let metadata: pw::metadata::Metadata = registry_clone
                        .bind(global)
                        .expect("failed to bind metadata");

                    let tx_clone = tx.clone();
                    let def_sink = Arc::clone(&device_lists.default_sink);
                    let def_source = Arc::clone(&device_lists.default_source);
                    let listener = metadata
                        .add_listener_local()
                        .property(move |subject, key, _type_, value| {
                            match key {
                                Some("default.audio.sink") => {
                                    println!("hihihihi");
                                    let name = value
                                        .and_then(|v| serde_json::from_str::<DefaultNodeValue>(v).ok())
                                        .map(|d| d.name);
                                    let _ = tx_clone.send(PipeWireEvent::DefaultDevice(DefaultDeviceEvent {
                                        id: subject,
                                        name: name.clone().unwrap_or_default(),
                                        class: "Sink".to_string(),
                                    }));
                                    *def_sink.lock().unwrap() = name.unwrap_or_default();
                                }
                                Some("default.audio.source") => {
                                    let name = value
                                        .and_then(|v| serde_json::from_str::<DefaultNodeValue>(v).ok())
                                        .map(|d| d.name);
                                    let _ = tx_clone.send(PipeWireEvent::DefaultDevice(DefaultDeviceEvent {
                                        id: subject,
                                        name: name.clone().unwrap_or_default(),
                                        class: "Source".to_string(),
                                    }));
                                    *def_source.lock().unwrap() = name.unwrap_or_default();
                                }
                                _ => {}
                            }
                            0
                        })
                        .register();
                    metadata_storage_clone.borrow_mut().replace((metadata, listener));
                }
                // volume / mute state updates
                ObjectType::Node => {
                    let Some(props) = &global.props else { return };
                    let media_class = props.get("media.class").unwrap_or("");
                    // println!("{:#?}", props);
                    if !matches!(media_class, "Audio/Sink" | "Audio/Source" | "Stream/Output/Audio" | "Stream/Input/Audio") {
                        return
                    };

                    let id = global.id;
                    let class = media_class.to_string();


                    let name = if matches!(media_class, "Audio/Sink" | "Audio/Source") {
                        props
                            .get("node.name")
                            .unwrap_or("unknown")
                            .to_string()
                    } else {
                        props
                            .get("application.name")
                            .or_else(|| props.get("node.name"))
                            .unwrap_or("unknown")
                            .to_string()
                    };

                    let description = props.get("node.description").unwrap_or("unknown").to_string();

                    let d = Device {
                        name:name.clone(),
                        description: description.clone(),
                        id,
                        mute: None,
                        channel_volumes: None
                    };

                    let a = Application {
                        id,
                        binary: String::new(),
                        app_name: name.clone(),
                        node_name: props.get("node.name").unwrap_or("unknown").to_string(),
                        media_name: String::new(),
                        channel_volumes: None,
                        mute: None
                    };

                    let tx = tx.clone(); // clone the Sender per-node, it's cheap (Arc internally)

                    let node: pw::node::Node = registry_clone.bind(global).expect("bind failed");

                    let sink_clone = sink_list_for_pw_clone.clone();
                    let source_clone = source_list_for_pw_clone.clone();
                    let app_clone = application_list_for_pw_clone.clone();

                    let listener = node
                        .add_listener_local()
                        .param(move |_seq, param_id, _index, _next, param| {
                            if param_id != ParamType::Props { return; }
                            let Some(pod) = param else { return };
                            let Ok((_, value)) = PodDeserializer::deserialize_any_from(pod.as_bytes()) else { return };
                            let Value::Object(obj) = value else { return };



                            let mut mute = None;
                            let mut vols = None;
                            for p in obj.properties {
                                match p.key {
                                    k if k == pw::spa::sys::SPA_PROP_mute => {
                                        if let Value::Bool(b) = p.value { mute = Some(b); }
                                    }
                                    k if k == pw::spa::sys::SPA_PROP_channelVolumes => {
                                        if let Value::ValueArray(pw::spa::pod::ValueArray::Float(a)) = p.value {
                                            vols = Some(a);
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            
                            if mute.is_some() || vols.is_some() {
                                match class.as_str() {
                                    "Audio/Sink" | "Audio/Source" => {
                                        let mut dev = d.clone();
                                        dev.mute = mute;
                                        dev.channel_volumes = vols.clone();
                                        let dev_clone = dev.clone();
                                        if class.as_str() == "Audio/Sink" {
                                            sink_clone.lock().unwrap().insert(id, dev);
                                        } else {
                                            source_clone.lock().unwrap().insert(id, dev);
                                        }

                                        // Non-blocking, sync send — safe from this callback.
                                        let _ = tx.send(PipeWireEvent::Volume(VolumeEvent {
                                            class: class.clone(),
                                            emitter: CombType::Device(dev_clone)
                                        }));
                                    }
                                    "Stream/Output/Audio" => {
                                        let mut app = a.clone();
                                        app.mute = mute;
                                        app.channel_volumes = vols.clone();
                                        let a_clone = app.clone();
                                        app_clone.lock().unwrap().insert(id, app);

                                        let _ = tx.send(PipeWireEvent::Volume(VolumeEvent {
                                            class: class.clone(),
                                            emitter: CombType::Application(a_clone)
                                        }));
                                    }
                                    "Stream/Input/Audio" => {

                                    }
                                    _ => {}
                                }
                            }
                        })
                        .register();

                    node.subscribe_params(&[ParamType::Props]);
                    nodes_clone.borrow_mut().insert(id, (node, listener));
                    }
                ObjectType::Client => {

                }
                _ => {}
            }
            
        })
        .global_remove(move |id| {
            application_list_for_pw_clone_remove.lock().unwrap().remove(&id);
            sink_list_for_pw_clone_remove.lock().unwrap().remove(&id);
            source_list_for_pw_clone_remove.lock().unwrap().remove(&id);
        })
        .register();

    
    let nodes_for_channel = nodes.clone();
    let cmd_receiver = pw_in_rx.attach(mainloop.loop_(), move |cmd| {
        handle_volume_command(cmd, &nodes_for_channel);
    });
    mainloop.run(); // blocks this thread forever
    Ok(())
}


fn handle_volume_command(cmd: VolumeCommand, nodes: &Rc<RefCell<HashMap<u32, (pw::node::Node, NodeListener)>>>) {
    match cmd {
        VolumeCommand::ToggleMuteSink { target } => {

        }
        _ => {}
    }
}