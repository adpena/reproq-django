# Handling Large Payloads

Reproq uses JSON for task arguments and results. While convenient, there are best practices to follow when dealing with large amounts of data.

## 1. Pass IDs, Not Objects

Avoid passing large Django model instances or large dictionaries as task arguments. Instead, pass the primary key and fetch the object inside the task.

**Bad:**
```python
@task
def process_user(user_obj): # user_obj could be huge
    pass
```

**Good:**
```python
@task
def process_user(user_id):
    user = User.objects.get(pk=user_id)
    # process user
```

## 2. Result Size Limits

The Go worker captures `stdout` from the Python executor. To prevent memory exhaustion, there is a default limit (usually 1MB) on the captured output. 

If your task needs to return a large amount of data, consider:
- Writing the data to a file or cloud storage (S3) and returning the URL/path.
- Saving the data to a specialized Result model in your database.

## 3. Serialization

Reproq computes `spec_hash` using Python's standard `json.dumps` with canonical sorting. Ensure your arguments and kwargs are JSON-serializable. For complex types like `Decimal` or `UUID`, convert them to strings (or add a serializer) before enqueueing.

## 4. Payload Transport Modes

The Go worker defaults to passing payloads over stdin. Avoid inline payload mode in production because it exposes payload data in process arguments. Production builds of the worker (`-tags prod`) reject `--payload-mode inline`.
