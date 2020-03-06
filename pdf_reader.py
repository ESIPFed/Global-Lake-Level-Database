
def read_pdf():

    import PyPDF2

    # open file as object so PyPDF2 can view pdf from object
    pdfFileObject = open('LakesReservoirs.pdf', 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObject)

    # Creates empty list that will be filled with text from pdf object
    raw_text_page = []
    for i in range(pdfReader.numPages):
        pageObj = pdfReader.getPage(i)
        raw_text_page.append(pageObj.extractText())
    # each full text of page is one item in the list, join all the pages together then split on new lines
    raw_text = ' '.join(raw_text_page)
    raw_text = raw_text.splitlines()
    return(raw_text)




def pdf_to_database(text_as_list):

    import pandas as pd
    import re

    del text_as_list[0:3]

    for i in text_as_list:
        if 'Total' in i:
            text_as_list.remove(i)

    keys = text_as_list[0:7]
    print(keys)

    text_data = text_as_list[7:]

    id_and_name = text_data[0::5]
    continent = text_data[1::5]
    country = text_data[2::5]
    type_lake = text_data[3::5]
    resolution_and_dates = text_data[4::5]

    # id_and_name and _resolution_and_dates need to be split into their own lists
    id = []
    name = []
    for j in id_and_name:
        match = re.compile("[^\W\d]").search(j)
        id.append(j[:match.start()])
        name.append(j[match.start():])

    resolution = []
    observation_date = []
    for j in resolution_and_dates:
        match = re.compile("[^\W\d]").search(j)
        resolution.append(j[:match.start()])
        observation_date.append(j[match.start():])


    print(id)
    print(len(id))
    print(len(name))
    print(len(continent))
    print(len(country))
    print(len(type_lake))
    print(len(resolution))
    print(len(observation_date))

    # # add lists as values to dictionary
    # # values_list = [id, name, continent, country, type_lake, resolution, observation_date]
    df = pd.DataFrame(list(zip(id, name, continent, country, type_lake, resolution, observation_date)),
                       columns = keys)
    #
    print(df)
    # # df = pd.DataFrame(data=values_list, columns=keys, index=values_list[0])
    # # print(df)
    # #print(simple_dict['Lake ID'])








all_pdf_text = read_pdf()
pdf_to_database(all_pdf_text)