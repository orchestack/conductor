# Orchestack Conductor

> 🚨 This is **prototype-level software** receiving frequent breaking changes as
> we improve our understanding of the problem-solution space.
>
> 👋 If you are interested in participating as an early **design partner** or
> would like to **request a demo**, please reach out to
> [hello@orchestack.com](mailto:hello@orchestack.com).

## User guide

...

## High-level design

To allow deducing changes from declarative representation, entities that have
state must also have a persistent identity. Tables and columns are renamed from
time to time, so using name as this identifier is undesirable. Instead, we
introduce UUID for tables and UID (a positive integer) for columns.

Entities which have their state entirely described by their definition do not
have the same requirement. Examples: pure functions, index declarations, certain
security policies.

## Contributing

```sh
ln -s ../../tools/pre-commit .git/hooks/pre-commit
```
