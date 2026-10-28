use testcontainers::GenericImage;

pub fn image() -> GenericImage {
    let reference = include_str!("../../src/indexer/fixtures/anvil-image.txt").trim();
    let (name, digest) = reference.rsplit_once(':').expect("Anvil image must have a digest");
    GenericImage::new(name, digest)
}
