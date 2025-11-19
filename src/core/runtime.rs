use std::collections::HashMap;
use std::marker::PhantomData;
use std::cell::UnsafeCell;
use super::types::*;
use std::any::Any;

// Runtime uses UnsafeCell for interior mutability
pub struct Runtime<E: Clone + Copy + 'static,> {

    // None indicates that the field at idx is not routed to a physical buffer
    pub(crate) buffer_ids: Vec<Option<PhysicalBuffer>>,
    pub(crate) buffers: HashMap<PhysicalBuffer, UnsafeCell<Vec<f32>>>,
    pub(crate) buffer_size: usize,
    
    pub(crate) execution_order: Vec<StoredComponent<E>>,
    _event_type: PhantomData<E>,

    update_rx: lock_freedom::channel::spsc::Receiver<Update<E>>,
    event_rx: lock_freedom::channel::spsc::Receiver<E>,

    // system buffers lookup table
    pub(crate) system_buffers: SystemBuffers,

    pub(crate) current_events: Vec<E>,

    pub(crate) states: Vec<Box<UnsafeCell<dyn Any + Send + 'static>>>,

}

impl<E: Clone + Copy> Runtime<E> {
    pub(crate) fn new(
        update_rx: lock_freedom::channel::spsc::Receiver<Update<E>>,
        event_rx: lock_freedom::channel::spsc::Receiver<E>,
        states: Vec<Box<UnsafeCell<dyn Any + Send + 'static>>>,
        buffer_size: usize,
    ) -> Self {

        let mut buffers = HashMap::new();

        buffers.insert(PhysicalBuffer(0), UnsafeCell::new(vec![0.0; buffer_size])); // System input
        buffers.insert(PhysicalBuffer(1), UnsafeCell::new(vec![0.0; buffer_size])); // System output
        
        Self {
            buffer_ids: Vec::new(),
            buffers: buffers,
            buffer_size: buffer_size,
            execution_order: Vec::new(),
            _event_type: PhantomData,
            update_rx,
            event_rx,
            system_buffers: SystemBuffers{input: None, output: None},
            current_events: Vec::new(),
            states: states,
        }
    }
    
    pub fn get_ctx(&self, handle: ContextHandle) -> Context<E> {
        Context {
            runtime: self,
            handle,
            buffer_size: self.buffer_size,
        }
    }
    
    pub fn tick(&mut self) {
        // Check for updates at the start of each tick
        while let Ok(update) = self.update_rx.recv() {
            (update.0)(self);
        }
        
        // Process events
        while let Ok((event)) = self.event_rx.recv() {
            self.current_events.push(event)
        }
        
        // Execute components
        for component in self.execution_order.iter(){
            match component {
                StoredComponent::User(UserComponent{component, context_handle, field_count, instance_name, processor_type}) => {
                    component(&self, *context_handle)
                },
                StoredComponent::System(SystemComponent{component_id, instance_name, buffer_idx}) => {
                    // system components need to tell write_from and read_to what buffer ids they were assigned
                    // this may not need to happen every tick though
                }
            }
        }
    }
    pub fn read_from(&mut self, input: &[f32]) {

        let input_buf = if let Some(input_comp) = self.system_buffers.input {
            self.buffer_ids[input_comp.buffer_idx.0].expect("fatal error: system buffer not allocated")
        } else {
            // fail silently, this means that the output wasn't routed.
            return
        };

        if let Some(buffer_cell) = self.buffers.get(&input_buf) {
            unsafe {
                let buffer = &mut *buffer_cell.get();
                let copy_len = input.len().min(buffer.len());
                buffer[..copy_len].copy_from_slice(&input[..copy_len]);
                
                // Fill remaining with zeros if input is shorter
                if copy_len < buffer.len() {
                    buffer[copy_len..].fill(0.0);
                }
            }
        }
    }

    pub fn write_to(&self, output: &mut [f32]) {
        
        let output_buf = if let Some(output_comp) = self.system_buffers.output {
            self.buffer_ids[output_comp.buffer_idx.0].expect("fatal error: system buffer not allocated")
        } else {
            panic!("output buffer allocated incorrectly"); // debugging panic
            // fail silently, this means that the output wasn't routed.
            return
        };

        if let Some(buffer_cell) = self.buffers.get(&output_buf) {
            unsafe {
                let buffer = &*buffer_cell.get();
                let copy_len = output.len().min(buffer.len());
                output[..copy_len].copy_from_slice(&buffer[..copy_len]);
                
                // Debug: Check if buffer has non-zero values
                let has_signal = buffer.iter().any(|&x| x != 0.0);
                if !has_signal {
                    println!("Warning: System output buffer is silent!");
                }
                
                // Fill remaining with zeros if output is longer
                if copy_len < output.len() {
                    output[copy_len..].fill(0.0);
                }
            }
        } else {
            println!("Warning: System output buffer not found!");
        }
    }

    pub fn process(&mut self, input: Option<&[f32]>, output: &mut[f32]) {
        if let Some(input) = input {
            self.read_from(input);
        };
        self.tick();
        self.write_to(output);
    }

}

unsafe impl<E: Clone + Copy + 'static> Send for Runtime<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::cell::UnsafeCell;

    #[derive(Clone, Copy)]
    struct TestEvent;

    fn test_send<T: Send>() {}

    #[test]
    fn test_any_send() {
        // Test individual components
        let _: Box<UnsafeCell<dyn Any + Send + 'static>> = Box::new(UnsafeCell::new(42i32));
        let states: Vec<Box<UnsafeCell<dyn Any + Send + 'static>>> = vec![
            Box::new(UnsafeCell::new(42i32)),
            Box::new(UnsafeCell::new(String::from("test"))),
        ];
        
        // Test that the states vector is Send
        test_send::<Vec<Box<UnsafeCell<dyn Any + Send + 'static>>>>();
        
        // Test a minimal runtime-like struct
        struct TestRuntime {
            states: Vec<Box<UnsafeCell<dyn Any + Send + 'static>>>,
        }
        
        let test_runtime = TestRuntime { states };
        
        // This should compile if Send is working
        test_send::<TestRuntime>();
        
        println!("All Send tests passed!");
    }
    
    #[test]
    fn test_runtime_send() {
        // Test that Runtime itself is Send
        test_send::<Runtime<TestEvent>>();
        println!("Runtime Send test passed!");
    }
}