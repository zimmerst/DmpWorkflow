'''
Created on Mar 25, 2016

@author: zimmer
'''
import random, string

def random_string_generator(size=8, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))
