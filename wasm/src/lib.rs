use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn start() -> Result<(), JsValue> {
    console_error_panic_hook::set_once();
    tracing_wasm::set_as_global_default();

    Ok(())
}

#[wasm_bindgen]
pub fn fend_math(expr: String) -> String {
    tracing::info!(?expr, "calculating");

    let mut context = fend_core::Context::new();
    let res = fend_core::evaluate(&expr, &mut context).unwrap();
    res.get_main_result().to_string()
}

#[wasm_bindgen]
pub enum TargetFormat {
    Png,
    // Jpeg(u8),
    // Pnm(PnmSubtype),
    Gif,
    Ico,
    Bmp,
    Farbfeld,
    Tga,
    OpenExr,
    Tiff,
    Avif,
    Qoi,
    // WebP,
    // Unsupported(String),
}

#[wasm_bindgen]
pub fn convert_image(buffer: Vec<u8>, into: TargetFormat) -> Vec<u8> {
    let format = match into {
        TargetFormat::Png => image::ImageOutputFormat::Png,
        TargetFormat::Gif => image::ImageOutputFormat::Gif,
        TargetFormat::Ico => image::ImageOutputFormat::Ico,
        TargetFormat::Bmp => image::ImageOutputFormat::Bmp,
        TargetFormat::Farbfeld => image::ImageOutputFormat::Farbfeld,
        TargetFormat::Tga => image::ImageOutputFormat::Tga,
        TargetFormat::OpenExr => image::ImageOutputFormat::OpenExr,
        TargetFormat::Tiff => image::ImageOutputFormat::Tiff,
        TargetFormat::Avif => image::ImageOutputFormat::Avif,
        TargetFormat::Qoi => image::ImageOutputFormat::Qoi,
    };

    let img = image::load_from_memory(&buffer).unwrap();
    let mut output = std::io::Cursor::new(Vec::new());
    img.write_to(&mut output, format).unwrap();
    output.into_inner()
}
