import contextvars

# Create a globally accessible ContextVar with a default of None
current_batch_id = contextvars.ContextVar('current_batch_id', default=None)