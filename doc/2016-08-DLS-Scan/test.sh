while read ARK; do
  curl -s http://192.168.45.24/$ARK | sha512sum - | awk -v ark="$ARK" '{print $1" *"ark}'
done < arks.txt

