use pipewire::spa::sys::{SPA_PARAM_Props, SPA_PARAM_ROUTE_device, SPA_PARAM_ROUTE_index, SPA_PARAM_ROUTE_props, SPA_PARAM_Route, SPA_PROP_channelVolumes, SPA_PROP_mute, SPA_TYPE_OBJECT_ParamRoute, SPA_TYPE_OBJECT_Props,};
use pipewire as pw;
use pw::spa::param::ParamType;
use pw::spa::pod::deserialize::PodDeserializer;
use pw::spa::pod::serialize::PodSerializer;
use pw::spa::pod::{Object, Pod, Property, PropertyFlags, Value, ValueArray};
use pw::types::ObjectType;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;

use crate::DeviceLists;

// ---------------------------------------------------------------------
// Events sent out over the websocket side
// ---------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum PipeWireEvent {
    Volume(VolumeEvent),
    DefaultDevice(DefaultDeviceEvent),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct VolumeEvent {
    pub class: String,
    pub emitter: CombType,
}
// updates to the default device (sink/source)
#[derive(Debug, Clone, serde::Serialize)]
pub struct DefaultDeviceEvent {
    pub id: u32,
    pub name: String,
    pub class: String, // sink/source
}

#[derive(serde::Deserialize)]
struct DefaultNodeValue {
    name: String,
}
// target device can be either specified by their ID or by their name
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Target {
    String(String),
    ID(u32)
}
// all available command related to volume (and therefore pipewire)
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "action", content = "data")]
pub enum VolumeCommand {
    ChangeSinkVolume { target: Target, delta: i32 },
    SetSinkVolume { target: Target, value: u8 },
    MuteSink { target: Target },
    UnmuteSink { target: Target },
    ToggleMuteSink { target: Target },
    ChangeSourceVolume { target: Target, delta: i32 },
    SetSourceVolume { target: Target, value: u8 },
    MuteSource { target: Target },
    UnmuteSource { target: Target },
    ToggleMuteSource { target: Target },
}
// representation of a node (sink/source)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub description: String,
    pub id: u32,
    pub device_id: Option<u32>,
    pub route_device: Option<i32>,
    pub mute: Option<bool>,
    pub channel_volumes: Option<Vec<f32>>
}
// representation of an application
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Application {
    pub binary: String,
    pub app_name: String,
    pub node_name: String,
    pub media_name: String,
    pub id: u32,
    pub mute: Option<bool>,
    pub channel_volumes: Option<Vec<f32>>
}
// nodes and apps behave somewhat similar so here is a combination enum should we want to accept either of them
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum CombType {
    Device(Node),
    Application(Application),
}

// ---------------------------------------------------------------------
// Route cache: (device global id, route_device) -> current route state
// ---------------------------------------------------------------------
// we need to store the route information to assosiate a node with a device (hardware)
// mute/volume changes are done on the hw device rather then the node

#[derive(Debug, Clone, Copy)]
struct RouteInfo {
    index: i32,
    hw_mute: bool,
    hw_volume: bool,
}

type ActiveRoutes = Arc<Mutex<HashMap<(u32, i32), RouteInfo>>>;

// (node global id) -> (device global id, route_device). Populated both at
// initial bind AND kept fresh via the Node's `info` event, since the ALSA/ACP
// monitor sometimes enriches a node's properties (device.id, card.profile.device)
// slightly AFTER the registry's initial `global` announcement — the registry
// listener only fires once per object, so relying on `global.props` alone can
// miss these keys depending on timing.
type NodeDeviceJoin = Arc<Mutex<HashMap<u32, (u32, i32)>>>;

// ---------------------------------------------------------------------
// Main thread entry point
// ---------------------------------------------------------------------
// this monstrosity listens to pipewire events and updates the locally stored states and emits Socket events

pub fn run_pipewire_thread(
    tx: UnboundedSender<PipeWireEvent>,
    pw_in_rx: pw::channel::Receiver<VolumeCommand>,
    device_lists: DeviceLists,
) -> Result<(), pw::Error> {
    pw::init();

    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;
    let registry = core.get_registry_rc()?;

    let nodes: Rc<RefCell<HashMap<u32, (Rc<pw::node::Node>, pw::node::NodeListener)>>> =
        Rc::new(RefCell::new(HashMap::new()));
    // Device (unlike Node) does not implement Clone, so we wrap it in Rc
    // ourselves to make cheap, reliable clones possible when extracting it
    // from the map.
    let devices: Rc<RefCell<HashMap<u32, (Rc<pw::device::Device>, pw::device::DeviceListener)>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let metadata_storage: Rc<RefCell<Option<(pw::metadata::Metadata, pw::metadata::MetadataListener)>>> =
        Rc::new(RefCell::new(None));
    let active_routes: ActiveRoutes = Arc::new(Mutex::new(HashMap::new()));
    let node_device_join: NodeDeviceJoin = Arc::new(Mutex::new(HashMap::new()));

    let registry_clone = registry.clone();
    let nodes_clone = nodes.clone();
    let devices_clone = devices.clone();
    let metadata_storage_clone = metadata_storage.clone();
    let active_routes_clone = active_routes.clone();
    let node_device_join_clone = node_device_join.clone();

    let application_list_c = Arc::clone(&device_lists.application_list);
    let application_list_c_remove = Arc::clone(&device_lists.application_list);
    let sink_list_c = Arc::clone(&device_lists.sink_list);
    let sink_list_c_remove = Arc::clone(&device_lists.sink_list);
    let source_list_c = Arc::clone(&device_lists.source_list);
    let source_list_c_remove = Arc::clone(&device_lists.source_list);
    let def_sink = Arc::clone(&device_lists.default_sink);
    let def_source = Arc::clone(&device_lists.default_source);

    let devices_clone_remove = devices.clone();
    let active_routes_clone_remove = active_routes.clone();
    let node_device_join_remove = node_device_join.clone();

    let _reg_listener = registry
        .add_listener_local()
        .global(move |global| match global.type_ {
            // -----------------------------------------------------
            // Default sink/source metadata
            // -----------------------------------------------------
            // we simply listen to changes of the default sink/source and update our state variables and send a socket msg
            ObjectType::Metadata => {
                let Some(props) = &global.props else { return };
                if props.get("metadata.name") != Some("default") {
                    return;
                }

                let metadata: pw::metadata::Metadata =
                    registry_clone.bind(global).expect("failed to bind metadata");

                let tx_clone = tx.clone();
                let def_sink_c = Arc::clone(&def_sink);
                let def_source_c = Arc::clone(&def_source);

                let listener = metadata
                    .add_listener_local()
                    .property(move |subject, key, _type_, value| {
                        match key {
                            Some("default.audio.sink") => {
                                let name = value
                                    .and_then(|v| serde_json::from_str::<DefaultNodeValue>(v).ok())
                                    .map(|d| d.name);
                                let _ = tx_clone.send(PipeWireEvent::DefaultDevice(DefaultDeviceEvent {
                                    id: subject,
                                    name: name.clone().unwrap_or_default(),
                                    class: "Sink".to_string(),
                                }));
                                *def_sink_c.lock().unwrap() = name.unwrap_or_default();
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
                                *def_source_c.lock().unwrap() = name.unwrap_or_default();
                            }
                            _ => {}
                        }
                        0
                    })
                    .register();

                metadata_storage_clone.borrow_mut().replace((metadata, listener));
            }

            // -----------------------------------------------------
            // Hardware devices (for Route-based mute/volume)
            // -----------------------------------------------------
            ObjectType::Device => {
                let Some(props) = &global.props else { return };
                if props.get("media.class") != Some("Audio/Device") {
                    return;
                }

                let id = global.id;
                let device: pw::device::Device =
                    registry_clone.bind(global).expect("bind device failed");
                let device = Rc::new(device); // Device isn't Clone; wrap it ourselves

                let active_routes_cb = active_routes_clone.clone();

                let listener = device
                    .add_listener_local()
                    .param(move |_seq, param_id, _index, _next, param| {
                        if param_id != ParamType::Route {
                            return;
                        }
                        let Some(pod) = param else { return };
                        let Ok((_, value)) = PodDeserializer::deserialize_any_from(pod.as_bytes())
                        else {
                            return;
                        };
                        let Value::Object(obj) = value else { return };

                        let mut route_index = None;
                        let mut route_device = None;
                        let mut hw_mute = false;
                        let mut hw_volume = false;

                        for p in &obj.properties {
                            match p.key {
                                k if k == SPA_PARAM_ROUTE_index => {
                                    if let Value::Int(i) = p.value {
                                        route_index = Some(i);
                                    }
                                }
                                k if k == SPA_PARAM_ROUTE_device => {
                                    if let Value::Int(i) = p.value {
                                        route_device = Some(i);
                                    }
                                }
                                _ => {}
                            }
                        }

                        // route.hw-mute / route.hw-volume live inside the
                        // "info" struct property, which the pod deserializer
                        // surfaces as a Value::Struct of alternating key/value
                        // strings. We scan for them defensively.
                        for p in &obj.properties {
                            if let Value::Struct(items) = &p.value {
                                for pair in items.windows(2) {
                                    if let (Value::String(k), Value::String(v)) = (&pair[0], &pair[1]) {
                                        if k == "route.hw-mute" {
                                            hw_mute = v == "true";
                                        }
                                        if k == "route.hw-volume" {
                                            hw_volume = v == "true";
                                        }
                                    }
                                }
                            }
                        }

                        if let (Some(idx), Some(dev)) = (route_index, route_device) {
                            active_routes_cb.lock().unwrap().insert(
                                (id, dev),
                                RouteInfo { index: idx, hw_mute, hw_volume },
                            );
                        }
                    })
                    .register();

                device.subscribe_params(&[ParamType::Route]);
                devices_clone.borrow_mut().insert(id, (device, listener));
            }

            // -----------------------------------------------------
            // Sinks / Sources / App streams
            // -----------------------------------------------------
            ObjectType::Node => {
                let Some(props) = &global.props else { return };
                let media_class = props.get("media.class").unwrap_or("");
                if !matches!(
                    media_class,
                    "Audio/Sink" | "Audio/Source" | "Stream/Output/Audio" | "Stream/Input/Audio"
                ) {
                    return;
                }

                let id = global.id;
                let class = media_class.to_string();

                let name = if matches!(media_class, "Audio/Sink" | "Audio/Source") {
                    props.get("node.name").unwrap_or("unknown").to_string()
                } else {
                    props
                        .get("application.name")
                        .or_else(|| props.get("node.name"))
                        .unwrap_or("unknown")
                        .to_string()
                };

                let description = props.get("node.description").unwrap_or("unknown").to_string();

                // Join keys to the backing hardware Device/Route, if any.
                let device_id: Option<u32> = props.get("device.id").and_then(|s| s.parse().ok());
                let route_device: Option<i32> =
                    props.get("card.profile.device").and_then(|s| s.parse().ok());

                let d = Node {
                    name: name.clone(),
                    description: description.clone(),
                    id,
                    mute: None,
                    channel_volumes: None,
                    device_id,
                    route_device,
                };

                let a = Application {
                    id,
                    binary: String::new(),
                    app_name: name.clone(),
                    node_name: props.get("node.name").unwrap_or("unknown").to_string(),
                    media_name: String::new(),
                    channel_volumes: None,
                    mute: None,
                };

                // Seed the join cache immediately if these were already present
                // at announcement time.
                if let (Some(dev_id), Some(rt_dev)) = (device_id, route_device) {
                    node_device_join_clone.lock().unwrap().insert(id, (dev_id, rt_dev));
                }

                let tx = tx.clone();
                let node: pw::node::Node = registry_clone.bind(global).expect("bind failed");
                let node = Rc::new(node); // Node isn't Clone; wrap it ourselves

                let sink_clone = sink_list_c.clone();
                let source_clone = source_list_c.clone();
                let app_clone = application_list_c.clone();
                let node_device_join_info = node_device_join_clone.clone();

                // The ALSA/ACP monitor sometimes fills in device.id /
                // card.profile.device slightly AFTER the initial registry
                // announcement. The `info` event fires whenever the node's
                // own properties change, so we re-check here to catch that.
                let listener = node
                    .add_listener_local()
                    .info(move |info: &pw::node::NodeInfoRef| {
                        let Some(p) = info.props() else { return };
                        let dev_id: Option<u32> = p.get("device.id").and_then(|s| s.parse().ok());
                        let rt_dev: Option<i32> =
                            p.get("card.profile.device").and_then(|s| s.parse().ok());
                        if let (Some(dev_id), Some(rt_dev)) = (dev_id, rt_dev) {
                            node_device_join_info.lock().unwrap().insert(id, (dev_id, rt_dev));
                        }
                    })
                    .param(move |_seq, param_id, _index, _next, param| {
                        if param_id != ParamType::Props {
                            return;
                        }
                        let Some(pod) = param else { return };
                        let Ok((_, value)) = PodDeserializer::deserialize_any_from(pod.as_bytes())
                        else {
                            return;
                        };
                        let Value::Object(obj) = value else { return };

                        let mut mute = None;
                        let mut vols = None;
                        for p in obj.properties {
                            match p.key {
                                k if k == SPA_PROP_mute => {
                                    if let Value::Bool(b) = p.value {
                                        mute = Some(b);
                                    }
                                }
                                k if k == SPA_PROP_channelVolumes => {
                                    if let Value::ValueArray(ValueArray::Float(a)) = p.value {
                                        vols = Some(a);
                                    }
                                }
                                _ => {}
                            }
                        }

                        if mute.is_none() && vols.is_none() {
                            return;
                        }

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

                                let _ = tx.send(PipeWireEvent::Volume(VolumeEvent {
                                    class: class.clone(),
                                    emitter: CombType::Device(dev_clone),
                                }));
                            }
                            "Stream/Output/Audio" | "Stream/Input/Audio" => {
                                let mut app = a.clone();
                                app.mute = mute;
                                app.channel_volumes = vols.clone();
                                let a_clone = app.clone();
                                app_clone.lock().unwrap().insert(id, app);

                                let _ = tx.send(PipeWireEvent::Volume(VolumeEvent {
                                    class: class.clone(),
                                    emitter: CombType::Application(a_clone),
                                }));
                            }
                            _ => {}
                        }
                    })
                    .register();

                node.subscribe_params(&[ParamType::Props]);
                nodes_clone.borrow_mut().insert(id, (node, listener));
            }

            _ => {}
        })
        .global_remove(move |id| {
            // should a node or app leave (disconnect/ stop playback) remove from state variables
            application_list_c_remove.lock().unwrap().remove(&id);
            sink_list_c_remove.lock().unwrap().remove(&id);
            source_list_c_remove.lock().unwrap().remove(&id);
            devices_clone_remove.borrow_mut().remove(&id);
            active_routes_clone_remove
                .lock()
                .unwrap()
                .retain(|(dev_id, _), _| *dev_id != id);
            node_device_join_remove.lock().unwrap().remove(&id);
            // TODO: send socket event that x left
        })
        .register();

    // all the stuff we need to handle incoming commands from the channel (ws -> pipewire client)

    let nodes_for_cmds = nodes.clone();
    let devices_for_cmds = devices.clone();
    let active_routes_for_cmds = active_routes.clone();
    let node_device_join_for_cmds = node_device_join.clone();
    let device_lists_for_cmds = DeviceLists {
        application_list: Arc::clone(&device_lists.application_list),
        sink_list: Arc::clone(&device_lists.sink_list),
        source_list: Arc::clone(&device_lists.source_list),
        default_sink: Arc::clone(&device_lists.default_sink),
        default_source: Arc::clone(&device_lists.default_source),
    };

    let _cmd_receiver = pw_in_rx.attach(mainloop.loop_(), move |cmd| {
        handle_volume_command(
            cmd,
            &nodes_for_cmds,
            &devices_for_cmds,
            &active_routes_for_cmds,
            &node_device_join_for_cmds,
            device_lists_for_cmds.clone(),
        );
    });

    mainloop.run(); // blocks this thread forever
    Ok(())
}

// ---------------------------------------------------------------------
// Handling of incoming commands from websocket server
// ---------------------------------------------------------------------

fn handle_volume_command(
    cmd: VolumeCommand,
    nodes: &Rc<RefCell<HashMap<u32, (Rc<pw::node::Node>, pw::node::NodeListener)>>>,
    devices: &Rc<RefCell<HashMap<u32, (Rc<pw::device::Device>, pw::device::DeviceListener)>>>,
    active_routes: &ActiveRoutes,
    node_device_join: &NodeDeviceJoin,
    device_lists: DeviceLists,
) {
    let def_sink = device_lists.default_sink.lock().unwrap().to_string();
    let def_source = device_lists.default_source.lock().unwrap().to_string();

    // TODO: handle app volume events
    match cmd {
        VolumeCommand::MuteSink { target } => with_sink(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_sink,
            |ctx| apply_mute(ctx, true),
        ),
        VolumeCommand::UnmuteSink { target } => with_sink(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_sink,
            |ctx| apply_mute(ctx, false),
        ),
        VolumeCommand::ToggleMuteSink { target } => with_sink(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_sink,
            |ctx| {
                let current = ctx.current_mute.unwrap_or(false);
                apply_mute(ctx, !current);
            },
        ),
        VolumeCommand::SetSinkVolume { target, value } => with_sink(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_sink,
            move |ctx| apply_volume(ctx, value as f32),
        ),
        VolumeCommand::ChangeSinkVolume { target, delta } => with_sink(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_sink,
            move |ctx| {
                let current = ctx.current_volume_percent.unwrap_or(100.0);
                let new_val = (current + delta as f32).clamp(0.0, 150.0);
                apply_volume(ctx, new_val);
            },
        ),

        VolumeCommand::MuteSource { target } => with_source(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_source,
            |ctx| apply_mute(ctx, true),
        ),
        VolumeCommand::UnmuteSource { target } => with_source(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_source,
            |ctx| apply_mute(ctx, false),
        ),
        VolumeCommand::ToggleMuteSource { target } => with_source(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_source,
            |ctx| {
                let current = ctx.current_mute.unwrap_or(false);
                apply_mute(ctx, !current);
            },
        ),
        VolumeCommand::SetSourceVolume { target, value } => with_source(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_source,
            move |ctx| apply_volume(ctx, value as f32),
        ),
        VolumeCommand::ChangeSourceVolume { target, delta } => with_source(
            nodes, devices, active_routes, node_device_join, &device_lists, target, def_source,
            move |ctx| {
                let current = ctx.current_volume_percent.unwrap_or(100.0);
                let new_val = (current + delta as f32).clamp(0.0, 150.0);

                apply_volume(ctx, new_val);
            },
        ),
    }
}

/// Everything needed to mutate a resolved sink/source target.
struct TargetCtx {
    node: Rc<pw::node::Node>,
    device: Option<(Rc<pw::device::Device>, i32 /* index */, i32 /* route_device */)>,
    current_mute: Option<bool>,
    current_volume_percent: Option<f32>,
    // whether the hardware route actually supports mute/volume; falls back
    // to Node/Props when false or when there's no backing device at all.
    hw_mute: bool,
    hw_volume: bool,
}

fn with_sink(
    nodes: &Rc<RefCell<HashMap<u32, (Rc<pw::node::Node>, pw::node::NodeListener)>>>,
    devices: &Rc<RefCell<HashMap<u32, (Rc<pw::device::Device>, pw::device::DeviceListener)>>>,
    active_routes: &ActiveRoutes,
    node_device_join: &NodeDeviceJoin,
    device_lists: &DeviceLists,
    target: Target,
    default_name: String,
    f: impl FnOnce(TargetCtx),
) {
    with_device_target(
        nodes, devices, active_routes, node_device_join, &device_lists.sink_list, target, default_name, f,
    );
}

fn with_source(
    nodes: &Rc<RefCell<HashMap<u32, (Rc<pw::node::Node>, pw::node::NodeListener)>>>,
    devices: &Rc<RefCell<HashMap<u32, (Rc<pw::device::Device>, pw::device::DeviceListener)>>>,
    active_routes: &ActiveRoutes,
    node_device_join: &NodeDeviceJoin,
    device_lists: &DeviceLists,
    target: Target,
    default_name: String,
    f: impl FnOnce(TargetCtx),
) {
    with_device_target(
        nodes, devices, active_routes, node_device_join, &device_lists.source_list, target, default_name, f,
    );
}

fn with_device_target(
    nodes: &Rc<RefCell<HashMap<u32, (Rc<pw::node::Node>, pw::node::NodeListener)>>>,
    devices: &Rc<RefCell<HashMap<u32, (Rc<pw::device::Device>, pw::device::DeviceListener)>>>,
    active_routes: &ActiveRoutes,
    node_device_join: &NodeDeviceJoin,
    snapshot: &Arc<Mutex<HashMap<u32, Node>>>,
    target: Target,
    default_name: String,
    f: impl FnOnce(TargetCtx),
) {
    let id = match target {
        Target::String(s) => find_device_id_by_name(snapshot, s),
        Target::ID(id) => {
            if id == 0 {
                find_device_id_by_name(snapshot, default_name)
            } else {
                Some(id)
            }
        }
    };

    let Some(id) = id else { return };

    let dev_info = { snapshot.lock().unwrap().get(&id).cloned() };
    let Some(dev_info) = dev_info else { return };

    let node: Option<Rc<pw::node::Node>> = {
        let nodes_ref = nodes.borrow();
        nodes_ref.get(&id).map(|(n, _)| Rc::clone(n))
    };
    let Some(node) = node else { return };

    // Resolve backing Device/Route, if any. We use the dedicated
    // node_device_join cache (kept fresh via the Node's `info` event) rather
    // than dev_info's fields, since those may have been captured before the
    // ALSA/ACP monitor finished enriching the node's properties.
    let mut device_target = None;
    let mut hw_mute = false;
    let mut hw_volume = false;

    let joined = node_device_join.lock().unwrap().get(&id).copied();

    if let Some((device_id, route_device)) = joined {
        let route_info = active_routes.lock().unwrap().get(&(device_id, route_device)).copied();
        if let Some(route_info) = route_info {
            let device: Option<Rc<pw::device::Device>> = {
                let devices_ref = devices.borrow();
                devices_ref.get(&device_id).map(|(d, _)| Rc::clone(d))
            };
            if let Some(device) = device {
                hw_mute = route_info.hw_mute;
                hw_volume = route_info.hw_volume;
                device_target = Some((device, route_info.index, route_device));
            }
        }
    }

    // pw internal volume is represented using a cubic function (because human hearing isnt linear in regards to loudness)
    // so volume% = cube_root(volume_pw) and volume_pw = (volume%)^3

    let current_volume_percent = dev_info
        .channel_volumes
        .as_ref()
        .and_then(|v| v.iter().cloned().fold(None, |m: Option<f32>, x| Some(m.map_or(x, |m| m.max(x)))))
        .map(|raw| raw.cbrt() * 100.0);

    let ctx = TargetCtx {
        node,
        device: device_target,
        current_mute: dev_info.mute,
        current_volume_percent,
        hw_mute,
        hw_volume,
    };

    f(ctx);
}

fn find_device_id_by_name(snapshot: &Arc<Mutex<HashMap<u32, Node>>>, name: String) -> Option<u32> {
    snapshot
        .lock()
        .unwrap()
        .iter()
        .find(|(_, dev)| dev.name == name)
        .map(|(id, _)| *id)
}

// ---------------------------------------------------------------------
// Applying mute / volume, choosing Route vs Props automatically
// ---------------------------------------------------------------------

fn apply_mute(ctx: TargetCtx, mute: bool) {
    if ctx.hw_mute {
        if let Some((device, index, route_device)) = &ctx.device {
            set_route_props(device, *index, *route_device, Some(mute), None);
            return;
        }
    }
    set_node_props(&ctx.node, vec![Property {
        key: SPA_PROP_mute,
        flags: PropertyFlags::empty(),
        value: Value::Bool(mute),
    }]);
}

fn apply_volume(ctx: TargetCtx, percent: f32) {
    let raw = (percent / 100.0).powi(3);
    let volumes = vec![raw, raw]; // stereo, flat; adjust if you support per-channel/balance

    if ctx.hw_volume {
        if let Some((device, index, route_device)) = &ctx.device {
            set_route_props(device, *index, *route_device, None, Some(volumes));
            return;
        }
    }
    set_node_props(&ctx.node, vec![Property {
        key: SPA_PROP_channelVolumes,
        flags: PropertyFlags::empty(),
        value: Value::ValueArray(ValueArray::Float(volumes)),
    }]);
}

// ---------------------------------------------------------------------
// Low-level pod builders
// ---------------------------------------------------------------------

fn set_node_props(node: &pw::node::Node, properties: Vec<Property>) {
    let result = PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &Value::Object(Object {
            type_: SPA_TYPE_OBJECT_Props,
            id: SPA_PARAM_Props,
            properties,
        }),
    );

    if let Ok((cursor, _)) = result {
        if let Some(pod) = Pod::from_bytes(&cursor.into_inner()) {
            node.set_param(ParamType::Props, 0, pod);
        }
    }
}

fn set_route_props(
    device: &pw::device::Device,
    route_index: i32,
    route_device: i32,
    mute: Option<bool>,
    volumes: Option<Vec<f32>>,
) {
    let mut inner_properties = Vec::new();
    if let Some(m) = mute {
        inner_properties.push(Property {
            key: SPA_PROP_mute,
            flags: PropertyFlags::empty(),
            value: Value::Bool(m),
        });
    }
    if let Some(v) = volumes {
        inner_properties.push(Property {
            key: SPA_PROP_channelVolumes,
            flags: PropertyFlags::empty(),
            value: Value::ValueArray(ValueArray::Float(v)),
        });
    }
    if inner_properties.is_empty() {
        return;
    }

    let inner_props = Object {
        type_: SPA_TYPE_OBJECT_Props,
        id: SPA_PARAM_Props,
        properties: inner_properties,
    };

    let route_obj = Object {
        type_: SPA_TYPE_OBJECT_ParamRoute,
        id: SPA_PARAM_Route,
        properties: vec![
            Property {
                key: SPA_PARAM_ROUTE_index,
                flags: PropertyFlags::empty(),
                value: Value::Int(route_index),
            },
            Property {
                key: SPA_PARAM_ROUTE_device,
                flags: PropertyFlags::empty(),
                value: Value::Int(route_device),
            },
            Property {
                key: SPA_PARAM_ROUTE_props,
                flags: PropertyFlags::empty(),
                value: Value::Object(inner_props),
            },
        ],
    };

    let result = PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &Value::Object(route_obj),
    );

    if let Ok((cursor, _)) = result {
        if let Some(pod) = Pod::from_bytes(&cursor.into_inner()) {
            device.set_param(ParamType::Route, 0, pod);
        }
    }
}