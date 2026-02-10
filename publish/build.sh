#!/bin/bash
set -e
set -u

# Configuration
RECIPES=(
    "release/ecoscope-workflows-ext-mnc"
)
PROJECT_ROOT="$(pwd)/src/ecoscope-workflows-ext-mnc"
OUTPUT_DIR="/tmp/ecoscope-workflows-custom/release/artifacts"
RECIPE_DIR="$(pwd)/publish/recipes"

echo ""
echo "=== Conda Package Build ==="
echo ""

# Set version
echo "→ Detecting version..."
export HATCH_VCS_VERSION=$(cd "$PROJECT_ROOT" && hatch version)
echo "  Version: $HATCH_VCS_VERSION"
echo ""

# Clean cache
echo "→ Cleaning Pixi cache..."
pixi clean cache --yes
echo ""

# Prepare directory
echo "→ Preparing output directory..."
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
echo "  Output: $OUTPUT_DIR"
echo ""

# Build recipes
echo "→ Building ${#RECIPES[@]} recipe(s)..."
echo ""

build_count=0
for rec in "${RECIPES[@]}"; do
    ((build_count++))
    recipe_file="${RECIPE_DIR}/${rec}.yaml"
    
    echo "  [$build_count/${#RECIPES[@]}] $rec"
    
    if [ ! -f "$recipe_file" ]; then
        echo "  ✗ Recipe not found: $recipe_file"
        exit 1
    fi
    
    rattler-build build \
        --recipe "$recipe_file" \
        --output-dir "$OUTPUT_DIR" \
        --channel https://prefix.dev/ecoscope-workflows \
        --channel conda-forge
    
    echo "  ✓ Built successfully"
    echo ""
done

# Summary
echo "=== Build Complete ==="
echo ""
artifact_count=$(find "$OUTPUT_DIR" -type f \( -name "*.conda" -o -name "*.tar.bz2" \) | wc -l)
echo "  Artifacts: $artifact_count"
echo "  Location: $OUTPUT_DIR"
echo ""