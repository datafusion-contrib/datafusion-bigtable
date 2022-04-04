use pyo3::prelude::*;

mod datasource;

#[pymodule]
fn _private(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<datasource::PyBigtableTable>()?;
    Ok(())
}
