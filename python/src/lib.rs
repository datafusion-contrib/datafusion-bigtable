use pyo3::prelude::*;

mod datasource;

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<datasource::PyBigtableTable>()?;
    Ok(())
}
