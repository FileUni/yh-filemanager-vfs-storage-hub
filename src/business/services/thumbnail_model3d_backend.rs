use anyhow::{Result, anyhow};
use std::path::Path;
use tokio::process::Command;
use yh_console_log::yhlog;

use super::{ThumbnailRuntimeConfig, ThumbnailRuntimeTypeConfig};

pub async fn render_model3d_thumbnail(
    model_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &Path,
    output: &Path,
) -> Result<bool> {
    let blender_path = cfg.get_tools().get_blender_path().trim();
    if blender_path.is_empty() {
        return Err(anyhow!("Blender path is empty"));
    }
    let temp_dir = input.parent().ok_or_else(|| anyhow!("Invalid temp dir"))?;
    let script_path = temp_dir.join("render_model3d_thumbnail.py");
    tokio::fs::write(&script_path, build_blender_script()).await?;

    let mut cmd = Command::new(blender_path);
    cmd.arg("--background")
        .arg("--factory-startup")
        .arg("--python")
        .arg(&script_path)
        .arg("--")
        .arg(input)
        .arg(output)
        .arg(cfg.get_thumb_size_px().to_string())
        .arg(cfg.get_thumb_quality().to_string());
    let timeout = model_cfg.get_timeout_secs();
    match run_command_with_timeout(cmd, timeout).await {
        Ok(value) => Ok(value && tokio::fs::metadata(output).await.is_ok()),
        Err(error) => {
            yhlog(
                "warn",
                &format!(
                    "Thumbnail tool failed (blender, path='{}'): {}",
                    blender_path, error
                ),
            );
            Err(error)
        }
    }
}

async fn run_command_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<bool> {
    let output =
        tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output()).await;
    match output {
        Err(_) => Err(anyhow!("Command timeout")),
        Ok(Err(error)) => Err(anyhow!(error)),
        Ok(Ok(output)) => {
            if output.status.success() {
                Ok(true)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                let message = stderr
                    .lines()
                    .chain(stdout.lines())
                    .map(str::trim)
                    .find(|line| !line.is_empty())
                    .unwrap_or("blender exited with non-zero status");
                Err(anyhow!(message.to_string()))
            }
        }
    }
}

fn build_blender_script() -> &'static str {
    r#"import math
import os
import sys

import bpy
from mathutils import Vector


def import_model(path):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".obj":
        try:
            bpy.ops.wm.obj_import(filepath=path)
        except Exception:
            bpy.ops.import_scene.obj(filepath=path)
    elif ext == ".stl":
        try:
            bpy.ops.wm.stl_import(filepath=path)
        except Exception:
            bpy.ops.import_mesh.stl(filepath=path)
    elif ext in {".gltf", ".glb"}:
        bpy.ops.import_scene.gltf(filepath=path)
    else:
        raise RuntimeError(f"Unsupported 3D model extension: {ext}")


def collect_renderables():
    return [obj for obj in bpy.context.scene.objects if obj.type in {"MESH", "CURVE", "SURFACE", "META", "FONT"}]


def compute_bounds(objects):
    bound_min = Vector((float("inf"), float("inf"), float("inf")))
    bound_max = Vector((float("-inf"), float("-inf"), float("-inf")))
    for obj in objects:
        for corner in obj.bound_box:
            world_corner = obj.matrix_world @ Vector(corner)
            bound_min.x = min(bound_min.x, world_corner.x)
            bound_min.y = min(bound_min.y, world_corner.y)
            bound_min.z = min(bound_min.z, world_corner.z)
            bound_max.x = max(bound_max.x, world_corner.x)
            bound_max.y = max(bound_max.y, world_corner.y)
            bound_max.z = max(bound_max.z, world_corner.z)
    return bound_min, bound_max


def setup_render(output_path, size, quality):
    scene = bpy.context.scene
    engine_items = [item.identifier for item in scene.render.bl_rna.properties["engine"].enum_items]
    for candidate in ("BLENDER_EEVEE_NEXT", "BLENDER_EEVEE", "CYCLES"):
        if candidate in engine_items:
            scene.render.engine = candidate
            break
    scene.render.resolution_x = size
    scene.render.resolution_y = size
    scene.render.resolution_percentage = 100
    scene.render.filepath = output_path
    scene.render.film_transparent = True
    ext = os.path.splitext(output_path)[1].lower()
    if ext in {".jpg", ".jpeg"}:
        scene.render.image_settings.file_format = "JPEG"
        scene.render.image_settings.quality = quality
    elif ext == ".webp":
        scene.render.image_settings.file_format = "WEBP"
        scene.render.image_settings.quality = quality
    else:
        scene.render.image_settings.file_format = "PNG"


def setup_scene(objects):
    bound_min, bound_max = compute_bounds(objects)
    center = (bound_min + bound_max) / 2.0
    extent = bound_max - bound_min
    radius = max(extent.x, extent.y, extent.z, 1.0)

    target = bpy.data.objects.new("Target", None)
    target.location = center
    bpy.context.scene.collection.objects.link(target)

    camera_data = bpy.data.cameras.new("ThumbnailCamera")
    camera = bpy.data.objects.new("ThumbnailCamera", camera_data)
    camera.location = Vector((radius * 2.4, -radius * 2.4, radius * 1.8))
    bpy.context.scene.collection.objects.link(camera)
    bpy.context.scene.camera = camera
    track = camera.constraints.new(type="TRACK_TO")
    track.target = target
    track.track_axis = "TRACK_NEGATIVE_Z"
    track.up_axis = "UP_Y"

    key_light = bpy.data.lights.new(name="KeyLight", type="SUN")
    key_light.energy = 3.0
    light_obj = bpy.data.objects.new(name="KeyLight", object_data=key_light)
    light_obj.rotation_euler = (math.radians(50), 0.0, math.radians(35))
    bpy.context.scene.collection.objects.link(light_obj)

    world = bpy.data.worlds.new("ThumbnailWorld")
    world.use_nodes = True
    background = world.node_tree.nodes.get("Background")
    if background is not None:
        background.inputs[0].default_value = (1.0, 1.0, 1.0, 1.0)
        background.inputs[1].default_value = 1.0
    bpy.context.scene.world = world


def main():
    argv = sys.argv
    if "--" not in argv:
        raise RuntimeError("Missing Blender script arguments")
    input_path, output_path, size, quality = argv[argv.index("--") + 1 : argv.index("--") + 5]
    bpy.ops.wm.read_factory_settings(use_empty=True)
    import_model(input_path)
    objects = collect_renderables()
    if not objects:
        raise RuntimeError("No renderable objects were imported")
    setup_render(output_path, int(size), int(quality))
    setup_scene(objects)
    bpy.ops.render.render(write_still=True)


if __name__ == "__main__":
    main()
"#
}
