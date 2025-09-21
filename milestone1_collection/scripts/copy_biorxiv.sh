#!/usr/bin/env bash
# Copy N .meca packages from bioRxiv's requester-pays bucket into your S3 bucket.
# - No tag/ACL copying (avoids AccessDenied on GetObjectTagging)
# - Parallel copy
# - Deterministic "first N" selection without broken-pipe warnings

set -Eeuo pipefail

### -------- CONFIG (edit or pass as env vars) --------
DEST_BUCKET="${DEST_BUCKET:-biorxiv-copy}"             # your bucket name
DEST_PREFIX="${DEST_PREFIX:-biorxiv/meca/}"            # where to put .meca in your bucket
SRC_BUCKET="${SRC_BUCKET:-biorxiv-src-monthly}"        # bioRxiv bucket (us-east-1)
SRC_PREFIX="${SRC_PREFIX:-Current_Content/September_2025/}"  # month or any prefix
N="${N:-1000}"                                         # how many .meca files to copy
PARALLEL="${PARALLEL:-16}"                             # xargs parallelism
PROFILE_OPT="${AWS_PROFILE:+--profile $AWS_PROFILE}"   # honors AWS_PROFILE if set
MANIFEST="${MANIFEST:-manifest.txt}"                   # output manifest (keys relative to SRC_BUCKET)
LOG="${LOG:-copy_biorxiv.log}"                         # log file
### ---------------------------------------------------

# --- checks ---
command -v aws >/dev/null || { echo "aws CLI not found"; exit 1; }
aws sts get-caller-identity $PROFILE_OPT >/dev/null || { echo "AWS auth failed"; exit 1; }
aws s3api head-bucket --bucket "$DEST_BUCKET" $PROFILE_OPT >/dev/null 2>&1 \
  || { echo "Destination bucket not accessible: $DEST_BUCKET"; exit 1; }

echo "Listing up to $N .meca keys under s3://$SRC_BUCKET/$SRC_PREFIX ..."
# Use 'aws s3 ls' + awk to select first N .meca keys—no head -> no broken-pipe.
aws s3 ls "s3://${SRC_BUCKET}/${SRC_PREFIX}" --recursive --request-payer requester $PROFILE_OPT \
  | awk -v N="$N" '$4 ~ /\.meca$/ {print $4; c++; if (c>=N) exit}' > "$MANIFEST"

COUNT=$(wc -l < "$MANIFEST" | tr -d ' ')
echo "Manifest ready: $MANIFEST ($COUNT keys)"

if [ "$COUNT" -eq 0 ]; then
  echo "No .meca files found under that prefix. Check SRC_PREFIX."
  exit 1
fi

echo "Starting parallel copy to s3://$DEST_BUCKET/$DEST_PREFIX  (log: $LOG)"
: > "$LOG"

# Export variables for the subshell invoked by xargs (zsh/bash compatible)
export SRC_BUCKET DEST_BUCKET DEST_PREFIX AWS_PROFILE

# Copy with --request-payer and --copy-props none (avoids GetObjectTagging)
# If you prefer quieter output, add --no-progress
cat "$MANIFEST" \
  | xargs -I{} -P "$PARALLEL" sh -c '
      src_key="$1"
      aws s3 cp "s3://${SRC_BUCKET}/${src_key}" \
                 "s3://${DEST_BUCKET}/${DEST_PREFIX}${src_key}" \
                 --request-payer requester \
                 --copy-props none \
                 --only-show-errors ${AWS_PROFILE:+--profile "$AWS_PROFILE"}
    ' _ {} | tee -a "$LOG"

echo "Verify: counting .meca at destination..."
aws s3 ls "s3://${DEST_BUCKET}/${DEST_PREFIX}${SRC_PREFIX}" $PROFILE_OPT --recursive \
  | grep -c '\.meca$' || true

echo "Done."
