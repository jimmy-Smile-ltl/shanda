from  pro2_google_scholar.get_author_by_title import GetAuthorByTitle
if __name__ == '__main__':
    table_name_read  = "article_TopPublications5Year_AI"
    get_author = GetAuthorByTitle(table_name_read = table_name_read)
    get_author.run()