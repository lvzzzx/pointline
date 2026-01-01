import pytest
import polars as pl
from abc import ABC

def test_base_service_is_abstract():
    # Attempt to import BaseService
    # This will fail initially because the file doesn't exist
    from pointline.services.base_service import BaseService
    
    # Ensure it inherits from ABC
    assert issubclass(BaseService, ABC)
    
    # Ensure it cannot be instantiated directly
    with pytest.raises(TypeError):
        BaseService()

def test_concrete_service_must_implement_abstract_methods():
    from pointline.services.base_service import BaseService
    
    class IncompleteService(BaseService):
        pass
        
    # Should fail to instantiate due to missing abstract methods
    with pytest.raises(TypeError):
        IncompleteService()

def test_base_service_lifecycle_execution():
    from pointline.services.base_service import BaseService
    
    # Create a concrete mock service
    class MockService(BaseService):
        def __init__(self):
            self.call_order = []
            
        def validate(self, data):
            self.call_order.append("validate")
            return data
            
        def compute_state(self, valid_data):
            self.call_order.append("compute_state")
            return valid_data
            
        def write(self, result):
            self.call_order.append("write")
            
    service = MockService()
    input_data = pl.DataFrame({"a": [1]})
    
    service.update(input_data)
    
    assert service.call_order == ["validate", "compute_state", "write"]
