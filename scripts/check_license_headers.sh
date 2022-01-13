set -e
echo "Checking that a Apache 2 license header is present at the top of all scala files in src/"

src_files_with_missing_license=$(find src -type f -name '*.scala' -exec bash -c '[ ! -z "$(head -1 {} | grep -v "// Licensed to the Apache Software Foundation (ASF) under one")" ] && echo "{}"' \;)
if [ ! -z "$src_files_with_missing_license" ]
then
    echo "Found unlicensed files!"
    echo $src_files_with_missing_license
    exit 1
fi
echo "All ok"
