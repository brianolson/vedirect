# Go tools for VE.Direct

```sh
go get github.com/brianolson/vedirect
```

```go
import "github.com/brianolson/vedirect"
```

Read serial from Victron Energy devices. Parse and make available as Go objects or JSON. Some utility code for dealing with the records.

See example apps cmd/vedump and cmd/vesend

A series of records is often compressed to be only the fields that change. For example, in the raw serial protocol the Product ID and Serial Number will be in every record printed every second, but when I return a series of records those are in the first record and not the next 999. If the voltage changes from one record to the next but the amperage doesn't, the amperage won't be in the next record. The full record for any time can be reconstructed by starting with the first record and applying each next record as an update.

A timestamp record "_t" is added to each record at the unix milliseconds* when the record was fully received and parsed from the serial port. ( * time since 1970-01-01 00:00:00 UTC )

## vedump

`cmd/vedump` is a trivial program that reads the statuses posted to the serial port and prints them as json-per-line records to stdandard out.

## vesend

`vesend` reads from a serial port and optionally:

* POST data to some URL
* server web interface and API with recent data
* poll MPPT internal temperature register via HEX protocol

`vesend` started as a tool to collect data from the serial port and upload it to a cloud server, but grew to do more.

```sh
vesend -dev /dev/serial/by-id/usb-VictronEnergy_BV_VE_Direct_cable_*-port0  -post 'https://127.0.0.1:1234/my_post_receiver' -send-period 1000 -z -serve :8080
```

```sh
curl http://127.0.0.1:8080/s/index.html
curl http://127.0.0.1:8080/ve.json
```

