cd ~/test-scalaxb/http/src/main/resources
# sed ':a;N;$!ba;s/\n/\r/g' adt-a08.hl7
# tr '\n' '\r' < adt-a08.hl7
curl -XPOST -d @adt-a08.hl7 http://localhost:9000/dump
