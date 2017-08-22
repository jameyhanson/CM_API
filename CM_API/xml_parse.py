'''
Created on Aug 22, 2017

Edit kms_acls.xml file


@author: jphanso
'''

import xml.etree.ElementTree as ET

def main():
    filename = 'kms-acls.xml'

    tree = ET.parse(filename)
    root = tree.getroot()
    
    for child in root:
        print(child.tag, child.attrib)
    
if __name__ == '__main__':
    main()