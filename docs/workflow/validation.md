# Validation

DART-Pipeline has a `validate` subcommand that runs basic validation (out of
range and checking presence of NA values). This can be called as

```shell
uv run dart-pipeline validate <file.nc>
```

This will print out variables that did not meet validation criteria:
1. Any NA values present
2. Value of variable outside range, as described by `valid_min` and `valid_max`
   variable attributes.

The variable ranges are quite permissive, and in the future users will be able
to configure narrower validation ranges via a configuration file.
