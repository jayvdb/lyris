use super::types::*;
use std::collections::HashMap;
use std::any::TypeId;
use std::collections::HashSet;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use crate::Runtime;
use lock_freedom::channel::spsc::{Sender, Receiver};
use super::router::{RoutingErr, PortHandle};
use super::processor::{Port, PortType, SystemInput, SystemOutput, input, output};

struct Ledger<E: Clone + Copy + 'static> {

    // this component depends on these logical buffers
    dependencies: HashMap<ComponentId, Vec<LogicalBuffer>>,
    // this logical buffer is depended on by these component
    anti_dependencies: HashMap<LogicalBuffer, Vec<ComponentId>>, 
    // this component produces this logical buffer
    produces: HashMap<ComponentId, Vec<LogicalBuffer>>,
    // this logical buffer is produced by this component
    anti_produces: HashMap<LogicalBuffer, ComponentId>,

    // Map component names to their stored data
    components: HashMap<(TypeId, &'static str), StoredComponent<E>>,

    // Map logical buffer keys to logical buffer IDså
    logical_buffer_map: HashMap<BufferKey, LogicalBuffer>,
    physical_buffer_map: HashMap<LogicalBuffer, PhysicalBuffer>,
    next_logical_buffer: usize,
    buffer_len: usize,

    scheduled_components: HashSet<ComponentId>,

}

struct SearchState {
    removed_component: ComponentId,
    removed_dependencies: Vec<(LogicalBuffer, ComponentId)>,
    buffers_allocated: Vec<LogicalBuffer>,
    buffers_freed: Vec<LogicalBuffer>,
    physical_allocated: HashMap<LogicalBuffer, PhysicalBuffer>,
    physical_freed: Vec<PhysicalBuffer>,
    max_seen: Option<i32>,
}

impl<E: Clone + Copy + 'static> Ledger<E> {
    fn new(components: HashMap<(TypeId, &'static str), StoredComponent<E>>, buffer_len: usize) -> Self {
        let input_key = input();
        let output_key = output();
        Self {
            dependencies: HashMap::new(),
            anti_dependencies: HashMap::new(),
            produces: HashMap::new(),
            anti_produces: HashMap::new(),
            components,
            logical_buffer_map: HashMap::new(),
            physical_buffer_map: HashMap::new(),
            next_logical_buffer: 10,
            buffer_len: buffer_len,

            scheduled_components: HashSet::new(),
        }
    }
    fn add_route(&mut self, from_key: BufferKey, to_key: BufferKey) -> 
            Result<(
                Vec<StoredComponent<E>>, 
                Vec<Option<PhysicalBuffer>>, // indexed by UserComponent.handle.component_id
                HashMap<PhysicalBuffer, Vec<f32>>,
                SystemBuffers,
            ), 
            RoutingErr> {

        // Create or get logical buffer for the connection
        let logical_buffer = if let Some(&existing) = self.logical_buffer_map.get(&from_key) {
            existing
        } else {
            let new_buffer = LogicalBuffer(self.next_logical_buffer);
            self.next_logical_buffer += 1;
            self.logical_buffer_map.insert(from_key, new_buffer);
            new_buffer
        };
        

        // Connect the to_key to the same logical buffer
        self.logical_buffer_map.insert(to_key, logical_buffer);

        // Set up the dependency relationship
        let from_component = self.get_component_id_for_buffer_key(from_key)?;
        let to_component = self.get_component_id_for_buffer_key(to_key)?;
        
        // to_component depends on logical_buffer
        self.dependencies.entry(to_component)
            .or_insert_with(Vec::new)
            .push(logical_buffer);
        
        // logical_buffer is depended on by to_component  
        self.anti_dependencies.entry(logical_buffer)
            .or_insert_with(Vec::new)
            .push(to_component);
        
        // from_component produces logical_buffer
        self.produces.entry(from_component)
            .or_insert_with(Vec::new)
            .push(logical_buffer);
            
        // logical_buffer is produced by from_component
        self.anti_produces.insert(logical_buffer, from_component);

        let mut best_order = Vec::new();
        let mut best_peak = i32::MAX;
        
        let mut current_allocated: i32 = 0;
        let mut to_explore = vec![];
        let mut to_undo = Vec::<SearchState>::new();
        let mut current_order = Vec::<ComponentId>::new();

        let mut current_free_buffer_stack = Vec::<PhysicalBuffer>::new();
        let mut next_physical_buffer: PhysicalBuffer = PhysicalBuffer(0);

        let mut current_buffer_allocations: HashMap<LogicalBuffer, PhysicalBuffer> = HashMap::new();
        let mut best_buffer_allocations: HashMap<LogicalBuffer, PhysicalBuffer> = current_buffer_allocations.clone();


        
        to_explore.push(self.list_kahns());
        
        while let Some(mut nodes) = to_explore.pop() {
            while let Some(node) = nodes.pop() {
                let mut state = self.explore(
                    node, 
                    &mut current_free_buffer_stack,
                    &mut next_physical_buffer,
                    &mut current_buffer_allocations
                );
                current_order.push(node);
                
                current_allocated += state.buffers_allocated.len() as i32;
                
                // Check if we're at a leaf (no more components to schedule)
                let next_available = self.list_kahns();
                if next_available.is_empty() && self.dependencies.is_empty() {
                    // We've scheduled everything!
                    if current_allocated < best_peak {
                        best_peak = current_allocated;
                        best_order = current_order.clone();
                        best_buffer_allocations = current_buffer_allocations.clone();
                    }
                } else if next_available.is_empty() && !self.dependencies.is_empty() {
                    // Cycle detected - we have dependencies left but no available nodes
                    for state in to_undo {
                        self.undo(state)
                    }
                    return Err(RoutingErr::CycleDetected);
                }
                
                // Calculate frees from previous component (if any)
                if let Some(last_state) = to_undo.last() {
                    current_allocated -= last_state.buffers_freed.len() as i32;
                    let last_max = last_state.max_seen.unwrap_or(0);
                    state.max_seen = Some(last_max.max(current_allocated));
                } else {
                    state.max_seen = Some(current_allocated);
                }
                
                to_undo.push(state);
                to_explore.push(nodes);
                nodes = next_available;
            }
            
            // Backtrack
            if let Some(state) = to_undo.pop() {
                current_order.pop();
                
                // Undo allocations - remove from hashmap
                for logical_buf in state.physical_allocated.keys() {
                    current_buffer_allocations.remove(logical_buf);
                }
                
                // Un-free buffers - remove them from free stack
                for _ in 0..state.physical_freed.len() {
                    current_free_buffer_stack.pop();
                }
                
                // Undo logical graph changes
                current_allocated -= state.buffers_allocated.len() as i32;
                if to_undo.last().is_some() {
                    current_allocated += to_undo.last().unwrap().buffers_freed.len() as i32;
                }
                self.undo(state);
            }
        }

        let input_handle = input();
        let input_key = BufferKey::System(SystemKey {
                marker: TypeId::of::<SystemInput>(),
                instance_name: input_handle.name,
            }
        );

        let output_handle = output();
        let output_key = BufferKey::System(SystemKey {
                marker: TypeId::of::<SystemOutput>(),
                instance_name: output_handle.name,
            }
        );

        let input_info = self.logical_buffer_map.get(&input_key)
            .and_then(|&logical_buffer| {
                best_buffer_allocations.get(&logical_buffer)
                    .map(|&physical_buffer| (logical_buffer, physical_buffer))
            });

        let output_info = self.logical_buffer_map.get(&output_key)
            .and_then(|&logical_buffer| {
                best_buffer_allocations.get(&logical_buffer)
                    .map(|&physical_buffer| (logical_buffer, physical_buffer))
            });

        // Get component IDs for system components
        let input_component_id = self.get_component_id_for_buffer_key(input_key)?;
        let output_component_id = self.get_component_id_for_buffer_key(output_key)?;

        self.convert_results(best_order, best_buffer_allocations, input_component_id, output_component_id)

    }
    
    fn list_kahns(&self) -> Vec<ComponentId> {
        // Get all component IDs from the components map
        let all_component_ids: Vec<ComponentId> = self.components.values()
            .map(|comp| match comp {
                StoredComponent::System(sys_comp) => sys_comp.component_id,
                StoredComponent::User(user_comp) => user_comp.context_handle.component_id,
            })
            .collect();
        
        // Find components that either:
        // 1. Have no dependencies at all (not in the dependencies map)
        // 2. Have all their dependencies satisfied (all buffers they depend on are produced)
        all_component_ids.into_iter()
            .filter(|&comp_id| {
                
                if self.scheduled_components.contains(&comp_id) {
                    return false;
                }
                
                match self.dependencies.get(&comp_id) {
                    None => true, // No dependencies = ready to run
                    Some(deps) => {
                        // All dependencies must be produced
                        deps.iter().all(|buffer| self.anti_produces.contains_key(buffer))
                    }
                }
            })
            .collect()
    }
    fn explore(&mut self, component_id: ComponentId, 
            free_stack: &mut Vec<PhysicalBuffer>, 
            next_physical_buffer: &mut PhysicalBuffer,
            current_buffer_allocations: &mut HashMap<LogicalBuffer, PhysicalBuffer>
    ) -> SearchState {
        
        // mark component as scheduled
        self.scheduled_components.insert(component_id);

        // Remove this component's dependencies 
        let consumed_buffers = self.dependencies.remove(&component_id).unwrap_or_default();
        
        // Remove this component from each buffer's consumer list
        let mut removed_dependencies = Vec::new();
        for buffer in &consumed_buffers {
            if let Some(consumers) = self.anti_dependencies.get_mut(buffer) {
                consumers.retain(|&comp_id| comp_id != component_id);
                removed_dependencies.push((*buffer, component_id));
            }
        }
        
        // Get buffers this component produces (these get allocated)
        let produced_buffers = self.produces.get(&component_id).cloned().unwrap_or_default();
        
        // Allocate physical buffers for produced logical buffers
        let mut physical_allocated = HashMap::new();
        for logical_buf in &produced_buffers {
            let physical_buf = if let Some(buf) = free_stack.pop() {
                buf
            } else {
                let buf = *next_physical_buffer;
                next_physical_buffer.0 += 1;
                buf
            };
            current_buffer_allocations.insert(*logical_buf, physical_buf);
            physical_allocated.insert(*logical_buf, physical_buf);
        }
        // Add produced buffers to anti_produces
        for buffer in &produced_buffers {
            self.anti_produces.insert(*buffer, component_id);
        }
        
        // Find buffers that can now be freed (no remaining consumers)
        let mut buffers_freed = Vec::new();
        let mut physical_freed = Vec::new();
        for buffer in &consumed_buffers {
            if let Some(consumers) = self.anti_dependencies.get(buffer) {
                if consumers.is_empty() {
                    buffers_freed.push(*buffer);
                    // Return the physical buffer to the free stack
                    if let Some(&physical_buf) = current_buffer_allocations.get(buffer) {
                        free_stack.push(physical_buf);
                        physical_freed.push(physical_buf);
                    }
                }
            }
        }
        
        SearchState {
            removed_component: component_id,
            removed_dependencies,
            buffers_allocated: produced_buffers,
            buffers_freed,
            physical_allocated,
            physical_freed,
            max_seen: None,
        }
    }

    fn undo(&mut self, state: SearchState) {
        // Restore component to dependencies
        let mut deps = Vec::new();
        for (buffer, comp_id) in &state.removed_dependencies {
            if comp_id == &state.removed_component {
                deps.push(*buffer);
            }
        }
        self.dependencies.insert(state.removed_component, deps);
        
        // Restore component to buffer consumer lists
        for (buffer, comp_id) in state.removed_dependencies {
            self.anti_dependencies.entry(buffer)
                .or_insert_with(Vec::new)
                .push(comp_id);
        }
        
        // Remove allocated buffers from anti_produces
        for buffer in state.buffers_allocated {
            self.anti_produces.remove(&buffer);
        }

        self.scheduled_components.remove(&state.removed_component);
    }

    fn get_component_id_for_buffer_key(&self, buffer_key: BufferKey) -> Result<ComponentId, RoutingErr> {
        // Handle both system and user components
        let component_key = match buffer_key {
            BufferKey::System(key) => {
                (key.marker, key.instance_name)
            },

            BufferKey::User(key) => {

                (key.processor_type, key.instance_name)
            }
        };

        let stored_component = self.components.get(&component_key)
            .ok_or(RoutingErr::ProcessorNotFound)?;

        let component_id = match stored_component {
            StoredComponent::System(sys_comp) => {
                sys_comp.component_id
            },
            StoredComponent::User(user_comp) => {
                user_comp.context_handle.component_id
            }
        };
        
        Ok(component_id)
    }

    fn create_component_id_map(&self) -> HashMap<ComponentId, &StoredComponent<E>> {
        self.components.values()
            .map(|comp| match comp {
                StoredComponent::System(sys_comp) => 
                    (sys_comp.component_id, comp),
                StoredComponent::User(user_comp) => 
                    (user_comp.context_handle.component_id, comp)
            })
            .collect()
    }

    fn convert_results(
        &self,
        best_order: Vec<ComponentId>,
        best_buffer_allocations: HashMap<LogicalBuffer, PhysicalBuffer>,
        input_component_id: ComponentId,
        output_component_id: ComponentId,
    ) -> Result<(
        Vec<StoredComponent<E>>, 
        Vec<Option<PhysicalBuffer>>,
        HashMap<PhysicalBuffer, Vec<f32>>,
        SystemBuffers,
    ), RoutingErr> {
        
        let component_map = self.create_component_id_map();
        let mut execution_order = create_execution_order(&component_map, &best_order);
        
        let buffer_map = assign_buffers_to_map(
            &mut execution_order,
            &best_buffer_allocations,
            &self.logical_buffer_map
        );
        
        let physical_buffers = initialize_physical_buffers(
            &buffer_map,
            &best_buffer_allocations,
            self.buffer_len
        );

        let system_buffers = create_system_buffers(
            &component_map,
            input_component_id,
            output_component_id,
        );
        
        Ok((execution_order, buffer_map, physical_buffers, system_buffers))
    }
}

fn create_system_buffers<E: Clone + Copy + 'static>(
    component_map: &HashMap<ComponentId, &StoredComponent<E>>,
    input_component_id: ComponentId,
    output_component_id: ComponentId,
) -> SystemBuffers {
    let input_component = component_map.get(&input_component_id)
        .and_then(|comp| match comp {
            StoredComponent::System(sys_comp) => Some(sys_comp),
            _ => None,
        });
    
    let output_component = component_map.get(&output_component_id)
        .and_then(|comp| match comp {
            StoredComponent::System(sys_comp) => Some(*sys_comp),
            _ => None,
        });
    
    SystemBuffers {
        input: input_component.copied(),
        output: output_component,
    }
}

fn create_execution_order<E: Clone + Copy + 'static>(
    component_map: &HashMap<ComponentId, &StoredComponent<E>>,
    best_order: &[ComponentId]
) -> Vec<StoredComponent<E>> {
    best_order.iter()
        .map(|id| **component_map.get(id)
            .expect("This error mode should not exist")
            )
        .collect()
}

fn initialize_physical_buffers(
    buffer_map: &[Option<PhysicalBuffer>],
    best_allocations: &HashMap<LogicalBuffer, PhysicalBuffer>,
    buffer_length: usize
) -> HashMap<PhysicalBuffer, Vec<f32>> {
    buffer_map.iter()
        .filter_map(|&buf| buf)
        .chain(best_allocations.values().copied())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|buf| (buf, vec![0.0; buffer_length]))
        .collect()
}

fn update_buffer_handle<E: Clone + Copy>(
    component: &mut StoredComponent<E>,
    buffer_idx: BufferIdx,
) {
    match component {
        StoredComponent::User(user_comp) => {
            user_comp.context_handle.buffer_ids_start = buffer_idx;
        },
        StoredComponent::System(sys_comp) => {
            sys_comp.buffer_idx = buffer_idx;
        }
    }
}
fn get_length<E: Clone + Copy>(component: &StoredComponent<E>) -> usize {
    match component {
        StoredComponent::User(user_comp) => user_comp.field_count,
        StoredComponent::System(_sys_comp) => 1, // system components only take 1 buffer id
    }
}

fn create_buffer_key_for_field<E: Clone + Copy + 'static>(
    component: &StoredComponent<E>, 
    field_idx: usize
) -> BufferKey {
    match component {
        StoredComponent::User(user_comp) => BufferKey::User(UserKey {
            processor_type: user_comp.processor_type,
            instance_name: user_comp.instance_name,
            field_idx,
        }),
        StoredComponent::System(sys_comp) => {
            let marker = match sys_comp.instance_name {
                "__system_input__" => TypeId::of::<SystemInput>(),
                "__system_output__" => TypeId::of::<SystemOutput>(),
                _ => unreachable!("This error should be unreachable. Unknown system component type"),
            };
            
            BufferKey::System(SystemKey {
                marker,
                instance_name: sys_comp.instance_name,
            })
        }
    }
}

fn lookup_physical_buffer(
    buffer_key: &BufferKey,
    logical_buffer_map: &HashMap<BufferKey, LogicalBuffer>,
    best_buffer_allocations: &HashMap<LogicalBuffer, PhysicalBuffer>
) -> Option<PhysicalBuffer> {
    logical_buffer_map.get(buffer_key)
        .and_then(|logical_buffer| best_buffer_allocations.get(logical_buffer))
        .copied()
}

fn assign_buffers_to_map<E: Clone + Copy + 'static>(
    execution_order: &mut [StoredComponent<E>],
    best_buffer_allocations: &HashMap<LogicalBuffer, PhysicalBuffer>,
    logical_buffer_map: &HashMap<BufferKey, LogicalBuffer>,
) -> Vec<Option<PhysicalBuffer>> {

    // Create buffer_ids array - this is a flat array where each component gets a contiguous slice
    let total_size = execution_order.iter()
            .map(get_length)
            .sum();
        let mut buffer_map = vec![None; total_size];

        execution_order.iter_mut()
            .scan(0, |buffer_idx, component| {
                update_buffer_handle(component, BufferIdx(*buffer_idx));
                let current_idx = *buffer_idx;
                *buffer_idx += get_length(component);
                Some((component, current_idx))
            })
            .flat_map(|(component, start_idx)| {
                let component_ref: &StoredComponent<E> = &*component;
                (0..get_length(component_ref))
                    .map(move |field_idx| (component_ref, start_idx, field_idx))
            })
            .for_each(|(component, global_idx, field_idx)| {
                let buffer_key = create_buffer_key_for_field(component, field_idx); 
                buffer_map[global_idx + field_idx] = lookup_physical_buffer(&buffer_key, logical_buffer_map, best_buffer_allocations);
            });

        buffer_map
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum BufferKey {
    System(SystemKey),
    User(UserKey),
}
// Internal routing key using TypeId
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct UserKey {
    processor_type: TypeId,
    instance_name: &'static str,
    field_idx: usize,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct SystemKey{
    marker: TypeId,
    instance_name: &'static str,
}

// Clerk handles all routing bookkeeping
pub(crate) struct Clerk<E: Clone + Copy + 'static> {

    ledger: Ledger<E>,
    // Channels for updates
    update_tx: lock_freedom::channel::spsc::Sender<Update<E>>,
    event_tx: lock_freedom::channel::spsc::Sender<E>,
}

impl<E: Clone + Copy + Debug + 'static> Clerk<E> {
    pub(crate) fn new(components: HashMap<(TypeId, &'static str), StoredComponent<E>>, buffer_len: usize, update_tx: Sender<Update<E>>, event_tx: Sender<E>) -> Self {
        Clerk {
            ledger: Ledger::new(components, buffer_len),
            update_tx,
            event_tx,
        }
    }
    pub(crate) fn add_route<P1: Port + 'static, P2: Port + 'static>(
        &mut self, 
        from: PortHandle<P1>, 
        to: PortHandle<P2>
    ) -> Result<(), RoutingErr> {


        // Convert buffer handles to buffer keys
        let from_key = match P1::port_type() {
            // The SystemInput port is an *output* port
            // *named* SystemInput
            PortType::SystemInput => BufferKey::System(SystemKey {
                marker: TypeId::of::<P1>(),
                instance_name: from.name,
            }),
            PortType::Output => BufferKey::User(UserKey {
                processor_type: from.processor_type,
                instance_name: from.name,
                field_idx: from.field_idx,
            }),
            _ => {
                return Err(RoutingErr::FromPortIsInput);
            }
        };
        
        let to_key = match P2::port_type() {
            // The SystemOutput port is an *input* port
            // *named* SystemOutput
            PortType::SystemOutput => BufferKey::System(SystemKey {
                marker: TypeId::of::<P2>(),
                instance_name: to.name,
            }),
            PortType::Input => BufferKey::User(UserKey {
                processor_type: to.processor_type,
                instance_name: to.name,
                field_idx: to.field_idx,
            }),
            _  => {
                return Err(RoutingErr::ToPortIsOutput);
            }
        };

        match self.ledger.add_route(from_key, to_key) {

            Ok((new_order, buffer_assignments, physical_buffers, system_buffers)) => {
                // Send update to runtime
                let update = Update(Box::new(move |runtime: &mut Runtime<E>| {
                    runtime.execution_order = new_order;
                    runtime.buffer_ids = buffer_assignments;
                    runtime.system_buffers = system_buffers;
                    
                    // Update physical buffers
                    for (physical_id, buffer_data) in physical_buffers {
                        runtime.buffers.insert(physical_id, UnsafeCell::new(buffer_data));
                    }
                }));
                
                self.update_tx.send(update).map_err(|_| RoutingErr::PortNotFound)?;
                Ok(())
            },
            Err(e) => {
                return Err(e)
            },
        }
    }
    
    pub(crate) fn send_event(&mut self, event: E) {
        self.event_tx.send(event).unwrap();
    }
    
}

