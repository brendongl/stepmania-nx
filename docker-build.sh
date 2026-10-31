#!/bin/bash
# Build stepmania-nx inside the devkitPro A64 container (persistent container "smnx-build" keeps installed portlibs).
set -e
cd "$(dirname "$0")"
if ! docker ps -a --format '{{.Names}}' | grep -qx smnx-build; then
  docker create --name smnx-build -v "$PWD":/work -w /work devkitpro/devkita64:latest sleep infinity >/dev/null
fi
docker start smnx-build >/dev/null
docker exec smnx-build bash -c 'dkp-pacman -S --needed --noconfirm switch-dev switch-portlibs dkp-toolchain-vars switch-cmake switch-pkg-config >/tmp/pacman.log 2>&1 || (tail -20 /tmp/pacman.log; exit 1); python3 -c "import jinja2,sys;sys.exit(0 if jinja2.__version__==\"3.0.3\" else 1)" 2>/dev/null || python3 -m pip install --break-system-packages --force-reinstall -q "jinja2==3.0.3" "markupsafe==2.0.1"'
docker exec -e JOBS=${JOBS:-12} smnx-build bash -c './build.sh 2>&1'
