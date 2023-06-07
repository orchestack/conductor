# Orchestack Conductor

> 🚨 This is **prototype-level software** receiving frequent breaking changes as
> we improve our understanding of the problem-solution space.
>
> 👋 If you are interested in participating as an early **design partner** or
> would like to **request a demo**, please reach out to
> [**hello@orchestack.com**](mailto:hello@orchestack.com).

## User guide

...

## High-level design

To allow deducing changes from declarative representation, entities that have
state must also have a persistent identity. Tables and columns are renamed from
time to time, so using name as this identifier is undesirable. Instead, a UUID
is introduced as identity for tables, and UID (a positive integer) for columns.

Entities which have their state entirely described by their definition do not
have the same requirement. Examples: pure functions, index declarations, certain
security policies.

### Score Definition Language

SQL Data Definition Language wasn't designed to be used declaratively. New
features need to be added. To avoid confusion with regular SQL and to make it
psychologically easier to diverge from it, a further step is made with a new
syntax: _Score Definition Language_, the `*.sd` (score definition) file
extension is adopted as well.

Example "score definition" file:

```sql
NAMESPACE northwind;

TABLE Orders
UUID 'FF780B98-5880-47C2-9817-F9F8600C3617'
(
    OrderID INTEGER UID 1,
    CustomerID TEXT UID 2,
    EmployeeID INTEGER UID 3
    -- ...
);
```

As you may notice, it is highly inspired by the
[SQL DDL](https://en.wikipedia.org/wiki/Data_definition_language). In this
example, the following differences can be spotted:

1. `NAMESPACE <identifier>` declaration: The namespace concept is used in place
   of database and schemas
1. Missing `CREATE` keyword: Everything is declarative, verbs are obsolete
1. Tables have `UUID <uuid>`: Tables have persistent state, they need a
   persistent identifier which users wouldn't be inclined to change
1. `UID <number>`: Similar to the table UUID rationale, but scoped to a single
   table rather than globally

### Ensembles

To go from SD (score definition) to a real DBMS, an adapter that can translate
between conductor's semantics to a particular DBMS is required. These adapters
are called "ensembles".

At this moment, only a single ensemble implementation is planned. It is built
on top of Apache DataFusion and Delta Lake.

## Roadmap

- [ ] **In progress** Core features design (declarative management for base
  tables and add/drop columns (options like `NOT NULL` and type changes are out
  of scope at this stage))
- [ ] **In progress** Fully functioning ensemble (w/ support for reading and
  writing DeltaLake data)
- [ ] AuthNZ (security policies design)
- [ ] Support for modifying column types, nullability, etc.
- [ ] Basic dataflow support
- [ ] [**Have an idea? Open an issue.**](https://github.com/orchestack/conductor/issues/new)

## Contributing

```sh
ln -s ../../tools/pre-commit .git/hooks/pre-commit
```
