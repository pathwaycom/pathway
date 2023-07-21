use pyo3::ffi::{
    PyEval_RestoreThread, PyEval_SaveThread, PyGILState_Ensure, PyGILState_GetThisThreadState,
    PyGILState_Release, PyGILState_STATE, PyThreadState,
};
use pyo3::{PyResult, Python};

pub struct PythonThreadState {
    gil_state: PyGILState_STATE,
    thread_state: *mut PyThreadState,
}

impl PythonThreadState {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // Verify that the thread does not have a Python thread state.
        // SAFETY: this function has no preconditions.
        let thread_state = unsafe { PyGILState_GetThisThreadState() };
        assert!(thread_state.is_null());

        // Acquire the GIL, this will create a Python thread state and attach it to the current
        // thread.
        // SAFETY: this function has no preconditions.
        let gil_state = unsafe { PyGILState_Ensure() };
        // Release the GIL, while keeping the Python thread state alive and attached to the
        // current thread (so that future calls to `PyGILState_Ensure`, like ones done by
        // `with_gil`, will reuse it).
        // SAFETY: we have just acquired the GIL.
        let thread_state = unsafe { PyEval_SaveThread() };

        let res = Self {
            gil_state,
            thread_state,
        };

        attach_python_tracer();

        res
    }
}

impl Drop for PythonThreadState {
    fn drop(&mut self) {
        unsafe {
            // Unfortunately `PyGILState_Check` is not available with the `abi3` feature, so we
            // cannot verify that the GIL is not held.
            // SAFETY: we assume that the value is dropped in an environment where the Python state
            // is the same as when the value was created, i.e. threads are allowed and GIL is not
            // acquired.
            PyEval_RestoreThread(self.thread_state);
            // This should release the GIL and destroy the Python thread state (as this is the call
            // that matches the call to `PyGILState_Ensure` that created it).
            // SAFETY: we have just re-acquired the GIL.
            PyGILState_Release(self.gil_state);

            // This thread should no longer have a Python thread state attached to it.
            // SAFETY: this function has no preconditions.
            let thread_state = PyGILState_GetThisThreadState();
            assert!(thread_state.is_null());
        }
    }
}

fn attach_python_tracer() {
    Python::with_gil(|py| -> PyResult<()> {
        let threading = py.import("threading")?;
        let trace = if py.version_info() >= (3, 10) {
            threading.call_method0("gettrace")?
        } else {
            // not public in old Python versions
            threading.getattr("_trace_hook")?
        };
        if !trace.is_none() {
            let sys = py.import("sys")?;
            sys.call_method1("settrace", (trace,))?;
        }
        Ok(())
    })
    .expect("attaching the tracer should not fail");
}
