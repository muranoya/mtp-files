mod app;
mod names;
mod selection;
mod transfer;
mod worker;

fn main() -> Result<(), slint::PlatformError> {
    app::run()
}
