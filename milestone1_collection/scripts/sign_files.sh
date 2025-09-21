# --- config ---
export AWS_PROFILE=default     # or omit if using default creds
DEST_BUCKET="biorxiv-copy"
UNPACK_PREFIX="biorxiv/unpacked/"   # where you synced the extracted contents
EXPIRES=360000                        # seconds (1h). Make longer/shorter as needed.
MANIFEST="manifest.txt"             # .meca keys (either from source or your bucket)

: > presigned_urls.tsv

while IFS= read -r meca_key; do
  # meca_key looks like either:
  #   Current_Content/September_2025/<uuid>.meca
  # or (if from your bucket):
  #   biorxiv/meca/Current_Content/September_2025/<uuid>.meca

  # Strip any leading 'biorxiv/meca/' so we get a relative path starting at Current_Content/...
  rel="${meca_key#biorxiv/meca/}"

  # Build the unpacked "content/" prefix for that package
  content_prefix="${UNPACK_PREFIX}${rel%.meca}/content/"

  # Find PDF(s) under that prefix and choose the largest (usually the paper PDF)
  objs_json=$(aws s3api list-objects-v2 \
      --bucket "$DEST_BUCKET" \
      --prefix "$content_prefix" \
      --query 'Contents[?ends_with(Key, `.pdf`)]|[].{Key:Key,Size:Size}' \
      --output json)

  count=$(echo "$objs_json" | jq 'length')
  if [ "$count" -eq 0 ]; then
    echo -e "${rel}\tNO_PDF_FOUND\t" >> presigned_urls.tsv
    echo "No PDF for: ${content_prefix}" 1>&2
    continue
  fi

  pdf_key=$(echo "$objs_json" | jq -r 'max_by(.Size).Key')
  url=$(aws s3 presign "s3://${DEST_BUCKET}/${pdf_key}" --expires-in "$EXPIRES")
  echo -e "${rel}\t${pdf_key}\t${url}" >> presigned_urls.tsv
  echo "OK: ${pdf_key}"
done < "$MANIFEST"

echo "Done. Wrote presigned URLs to presigned_urls.tsv"
