#!/bin/bash
set -e
set -u

# Parse arguments
workflow=$1
shift
flags=$*

echo ""
echo "=== Workflow Recompilation ==="
echo ""

# Update environment
echo "→ Updating compile environment..."
pixi update --manifest-path pixi.toml -e compile
echo ""

# Initialize graphviz
echo "→ Initializing graphviz..."
pixi run --manifest-path pixi.toml -e compile dot -c
echo ""

# Compile workflow
echo "→ Compiling workflow..."
echo "  Workflow: workflows/${workflow}/spec.yaml"
echo "  Flags: --clobber ${flags}"
echo ""

pixi run --manifest-path pixi.toml -e compile \
    ecoscope-workflows compile \
    --spec "workflows/${workflow}/spec.yaml" \
    --clobber ${flags}

echo ""
echo "✓ Compilation complete"
echo ""