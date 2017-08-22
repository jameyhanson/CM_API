'''
Created on Aug 22, 2017

Ref: https://www.tutorialspoint.com/python/python_xml_processing.htm

Edit kms_acls.xml file

See: https://docs.python.org/3/library/xml.dom.html


@author: jphanso
'''

from xml.dom.minidom import parseString
import xml.dom.minidom

def main():
    filename = 'kms-acls.xml'
    file_handle = open(filename)
    non_xml_string = file_handle.read()   
    xml_string = '<doc_root>\n' + non_xml_string + '\n</doc_root>'
    
    DOMTree = parseString(xml_string)
    elements = DOMTree.documentElement
     
    sub_elements = elements.getElementsByTagName('property')
    
    i = 0
    for element in sub_elements:
        prop_name = element.getElementsByTagName('name')[0]
        i += 1
        print(str(i))    
        print(type(element))
        print(prop_name.childNodes[0].data)
        
        prop_value = element.getElementsByTagName('value')[0]
        try:
            prop_value.childNodes[0].data
        except:
            print('##### no value #####')
        else:
            print(prop_value.childNodes[0].data)
        print('\n')
    
if __name__ == '__main__':
    main()