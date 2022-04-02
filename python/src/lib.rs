use pyo3::prelude::*;

mod datasource;
mod errors;
mod utils;

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<datasource::PyBigtableTable>()?;
    Ok(())
}
