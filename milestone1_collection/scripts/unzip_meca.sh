export AWS_PROFILE=default    # or omit if using default creds
DEST_BUCKET="biorxiv-copy"
SRC_PREFIX="biorxiv/meca/"                 # where your .meca files live (in your bucket)
OUT_PREFIX="biorxiv/unpacked/"

while IFS= read -r key; do
  echo "Unpacking: $key"
  tmpdir="$(mktemp -d)" || exit 1
  aws s3 cp "s3://${DEST_BUCKET}/${key}" "$tmpdir/file.meca" --only-show-errors || { rm -rf "$tmpdir"; continue; }
  unzip -q -d "$tmpdir/unpacked" "$tmpdir/file.meca" || { rm -rf "$tmpdir"; continue; }

  rel="${key#$SRC_PREFIX}"
  out_key_prefix="${OUT_PREFIX}${rel%.meca}/"

  aws s3 sync "$tmpdir/unpacked/" "s3://${DEST_BUCKET}/${out_key_prefix}" --only-show-errors
  rm -rf "$tmpdir"
done < meca_keys.txt