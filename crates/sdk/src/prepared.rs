use protocol::{PreparedSchema, ValidationError};
use serde_json::Value;

#[cfg(test)]
thread_local! {
    static WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
pub(super) fn reset_work() {
    WORK.with(|work| work.set((0, 0)));
}

#[cfg(test)]
pub(super) fn work() -> (usize, usize) {
    WORK.with(std::cell::Cell::get)
}

fn prepare(schema: &Value) -> Result<PreparedSchema, ValidationError> {
    #[cfg(test)]
    WORK.with(|work| {
        let (preparations, checks) = work.get();
        work.set((preparations + 1, checks));
    });
    PreparedSchema::new(schema)
}

// Raw caller APIs previously checked output/error structure only when a matching
// payload arrived. Capture failures too, preserving that timing for streams.
pub(super) struct StreamValidation {
    output: Result<PreparedSchema, ValidationError>,
    error: Result<PreparedSchema, ValidationError>,
}

impl StreamValidation {
    pub(super) fn new(definition: &Value) -> Self {
        Self {
            output: prepare(&definition["output"]),
            error: prepare(&definition["error"]),
        }
    }

    pub(super) fn output_valid(&self, value: &Value) -> bool {
        #[cfg(test)]
        WORK.with(|work| {
            let (preparations, checks) = work.get();
            work.set((preparations, checks + 1));
        });
        self.output
            .as_ref()
            .is_ok_and(|schema| schema.validate_value(value).is_ok())
    }

    pub(super) fn error_valid(&self, value: &Value) -> bool {
        #[cfg(test)]
        WORK.with(|work| {
            let (preparations, checks) = work.get();
            work.set((preparations, checks + 1));
        });
        self.error
            .as_ref()
            .is_ok_and(|schema| schema.validate_value(value).is_ok())
    }
}
