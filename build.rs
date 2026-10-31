fn main() {
    // スタイルを固定し、環境に Qt があっても見た目が変わらないようにする
    let config = slint_build::CompilerConfiguration::new().with_style("fluent".into());
    slint_build::compile_with_config("ui/app.slint", config).expect("Slint のコンパイルに失敗");
}
