
import pytest

def foo(x):
	return x + 2

def test_foo():
	assert foo(3) == 6