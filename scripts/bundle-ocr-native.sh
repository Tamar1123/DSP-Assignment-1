#!/bin/bash
# Bundles the Tesseract/Leptonica native libraries into the repository so the worker jar is
# self-contained: no node has to apt/yum install an OCR engine at boot.
#
# The libraries are taken from the Ubuntu 22.04 (jammy) packages that match the AMI used by
# utils/AWS.ami. They are only the runtime shared objects (no headers, no dev files); the
# language data comes from the tess4j jar, which already ships tessdata/eng.traineddata.
#
# Re-run after changing the AMI to a different Ubuntu release: the .so files must be built
# against a glibc no newer than the target image's.
#
# Usage: scripts/bundle-ocr-native.sh
set -euo pipefail
cd "$(dirname "$0")/.."

DEST=src/main/resources/ocr-native/linux-x86-64
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Runtime packages providing the closure of libtesseract.so and liblept.so.5.
PACKAGES="
libtesseract4
liblept5
libtiff5
libjpeg-turbo8
libpng16-16
libgif7
libopenjp2-7
libwebp7
libwebpmux3
libwebpdemux2
libjbig0
libdeflate0
"

echo "Downloading Ubuntu 22.04 packages..."
( cd "$WORK" && apt-get download $PACKAGES >/dev/null )

for deb in "$WORK"/*.deb; do
    dpkg-deb -x "$deb" "$WORK/root"
done

mkdir -p "$DEST"
rm -f "$DEST"/*.so*

# Sonames exactly as the shipped libraries request them (DT_NEEDED / JNA lookup).
copy() {
    local src=$1 name=$2
    cp -L "$WORK/root/usr/lib/x86_64-linux-gnu/$src" "$DEST/$name"
}

copy libtesseract.so.4   libtesseract.so
copy liblept.so.5        liblept.so.5
copy libtiff.so.5        libtiff.so.5
copy libjpeg.so.8        libjpeg.so.8
copy libpng16.so.16      libpng16.so.16
copy libgif.so.7         libgif.so.7
copy libopenjp2.so.7     libopenjp2.so.7
copy libwebp.so.7        libwebp.so.7
copy libwebpmux.so.3     libwebpmux.so.3
copy libwebpdemux.so.2   libwebpdemux.so.2
copy libjbig.so.0        libjbig.so.0
copy libdeflate.so.0     libdeflate.so.0

{
    echo "Bundled from Ubuntu 22.04 (jammy) amd64 packages:"
    for deb in "$WORK"/*.deb; do
        echo "  $(basename "$deb" .deb)"
    done
    echo
    echo "System libraries still expected on the node: libc6, libstdc++6, libgcc-s1,"
    echo "libarchive13 (an apt dependency), libgomp1, zlib1g, liblzma5, libzstd1."
} > "$DEST/MANIFEST.txt"

echo "Wrote $(ls -1 "$DEST"/*.so* | wc -l) libraries to $DEST"
du -sh "$DEST"
