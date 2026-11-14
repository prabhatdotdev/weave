[← All examples](../README.md) · [Library guide](../../README.md)

# Protocol Buffers

Encode a protobuf value into `core.Message`, then decode it with the same
codec. This example needs no broker, Docker, generated files, or `protoc`.

## Run

With Go 1.25+, from the repository root:

```sh
go run ./examples/protobuf
```

Expected output:

```text
hello
```

## Adapt it

The example uses `wrapperspb.StringValue`, a standard protobuf type.
`codec.MarshalMessage(codec.Protobuf, value)` returns a message with the
protobuf content type; `codec.UnmarshalMessage` fills a destination value.
Both require values implementing `proto.Message`, normally pointers to generated
types.

Replace the wrapper with your generated schema type, then pass the encoded
message to either transport. Encoding does not establish a broker connection or
provide schema compatibility checks; producers and consumers own their schema
agreement. See [JSON](../json) for a broker-backed round trip and the
[codec reference](../../docs/API.md#codecs) for validation helpers.
