use super::types::*;
use crate::{core::Clerk, Runtime, Router};
use std::collections::HashMap;
use std::any::{TypeId, Any};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};
use std::fmt::Debug;
use super::processor::{Processor, SystemInput, SystemOutput};

pub struct Builder<E: Clone + Copy + Debug + 'static>{
    components: Vec<(TypeId, &'static str, StoredComponent<E>)>,
    next_component_id: usize,
    buffer_size: usize,
    states: Vec<Box<UnsafeCell<dyn Any + Send + 'static>>>,
}

impl<E: Clone + Copy + Debug> Builder<E> {
    pub fn new() -> Self {
        Self {
            components: Vec::new(),
            next_component_id: 0,
            buffer_size: 512,
            states: Vec::new(),
        }
    }
    
    pub fn add_processor<P: Processor>(
        mut self,
        processor: P,
        instance_name: &'static str,
    ) -> Self {
        let component_id = ComponentId(self.next_component_id);
        self.next_component_id += 1;
        
        let handle = ContextHandle {
            component_id,
            buffer_ids_start: BufferIdx(0), // Will be set during build
            slot_ids_start: self.states.len()
        };

        // Create a wrapper function that calls the processor's call method
        let component_fn = |runtime: &Runtime<E>, handle: ContextHandle| {
            P::call(runtime, handle)
        };

        let stored = UserComponent {
            component: component_fn,
            context_handle: handle,
            field_count: P::buffers_count(),
            instance_name,
            processor_type: TypeId::of::<P>(),
        };

        self.components.push((TypeId::of::<P>(), instance_name, StoredComponent::User(stored)));
        self
    }

    pub fn add<P: Processor>(mut self, processor: P) -> Self {
        let component_id = ComponentId(self.next_component_id);
        self.next_component_id += 1;
        
        let handle = ContextHandle {
            component_id,
            buffer_ids_start: BufferIdx(0), // Set during build
            slot_ids_start: self.states.len(),
        };

        self.states.extend(P::create_states());

        // use type name for unique, hashable identifier
        let instance_name = std::any::type_name::<P>();
        
        let stored = UserComponent {
            component: P::call,
            context_handle: handle,
            field_count: P::buffers_count(),
            instance_name: instance_name,
            processor_type: TypeId::of::<P>(),
        };
        
        self.components.push((TypeId::of::<P>(), instance_name, StoredComponent::User(stored)));
        self
    }

    pub fn buffer_length(mut self, length: usize) -> Self {
        self.buffer_size = length;
        self
    }
    
    pub fn build(self) -> (Runtime<E>, Router<E>) {
        let (update_tx, update_rx) = lock_freedom::channel::spsc::create();
        let (event_tx, event_rx) = lock_freedom::channel::spsc::create();
        
        let mut components = HashMap::new();

        let input_component = StoredComponent::System(SystemComponent {
            component_id: ComponentId(0),
            instance_name: "__system_input__",
            buffer_idx: BufferIdx(0), // will be configured during routing!
        });
        components.insert((TypeId::of::<SystemInput>(), "__system_input__"), input_component);

        let output_component = StoredComponent::System(SystemComponent {
            component_id: ComponentId(1), 
            instance_name: "__system_output__",
            buffer_idx: BufferIdx(0), // will be configured during routing!
        });
        components.insert((TypeId::of::<SystemOutput>(), "__system_output__"), output_component);
        
        for (type_id, name, stored) in self.components {
            components.insert((type_id, name), stored);
        }
        
        let clerk = Arc::new(Mutex::new(Clerk::new(components, self.buffer_size, update_tx, event_tx)));
        
        let router = Router {
            clerk: Arc::clone(&clerk),
        };
        
        let runtime = Runtime::new(update_rx, event_rx, self.states, self.buffer_size);
        
        (runtime, router)
    }

}