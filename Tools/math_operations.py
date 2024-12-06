from functools import wraps
import inspect

def with_context(func):
    """
    Decorator that automatically handles context management for function parameters and return values.
    It extracts parameters from context and puts the return value back into context with function name as key.
    """
    @wraps(func)
    def wrapper(context):
        # Get the function's parameter names
        params = inspect.signature(func).parameters
        
        # Extract parameters from context
        kwargs = {}
        for param_name in params:
            kwargs[param_name] = context.get(param_name)
        
        # Call the original function
        result = func(**kwargs)
        
        # If the function returns None, return empty dict
        if result is None:
            return {}
            
        # If the function returns a dict, use it as is
        if isinstance(result, dict):
            return result
            
        # Otherwise, store the result with function name as key
        return {f'{func.__name__}_result': result}
    
    return wrapper

@with_context
def add_numbers(a, b):
    """Add two numbers from context"""
    return a + b

@with_context
def multiply_by_two(add_numbers_result):
    """Multiply the result by 2"""
    return add_numbers_result * 2

@with_context
def format_result(multiply_by_two_result):
    """Format the result as a string"""
    return {'formatted': f"The final result is: {multiply_by_two_result}"}


if __name__ == "__main__":
    context = {
        'a': 1,
        'b': 2
    }
    result = add_numbers(context)
    print(result)