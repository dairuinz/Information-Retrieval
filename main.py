import searcher
from uploader import *
from searcher import *
import os

#sudo systemctl start elasticsearch                          STARTS ELASTICSEARCH
#sudo -i service kibana start                                STARTS KIBANA
#sudo -i service kibana stop                                 TERMINATES KIBANA


print('\n~~ Information Retrieval ~~\n')
indx = 'books'


def main():

    es_cnfg()
    searcher.users()

    id = input('\nPlease enter user id: ')
    a = input('\nInitiate a search? y/n: ')

    if a == 'y':
        search_for = input("Search: ")
        book_sorter(userid(id), search(indx, search_for))

    else:
        pass

if __name__ == '__main__':
    main()

