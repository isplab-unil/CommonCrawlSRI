#!/usr/bin/env python

import pandas as pd
import urllib.request

import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
import re

from matplotlib.colors import ListedColormap, LinearSegmentedColormap

matplotlib.rc('font', **{'family': 'serif', 'size': '16'})
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

plt.rcParams["font.size"] = "16"
plt.rcParams["figure.figsize"] = (10, 5)

data = pd.read_csv('../data/survey/sri.csv', delimiter=',')

# Data cleaning - dropped the first two rows
data.drop(data.index[[0, 1]], inplace=True)

# Drop unnecessay columns
data.drop(["StartDate", "EndDate", "Status", "Progress", "Duration (in seconds)", "RecordedDate",
           "DistributionChannel", "UserLanguage", "Q15_7_TEXT", "Q40_6_TEXT", "Q41_4_TEXT",
           "Q44_1", "Q21_6_TEXT", "Q16_62_TEXT", "Q26_45_TEXT", "Q17_47_TEXT", "Q59_14_TEXT",
           "Q36", "Q35", "Q37", "Q24", "Q64", "Q25", "Q65", "Q39", "Q42", "Q43"], axis=1, inplace=True)

# Create question dictionary that maps to question labels
question_dict = {'Q14': 'Age', 'Q32': 'EnglishQuestionnaire', 'Q15': 'Employment', 'Q40': 'SecurityBackground',
                 'Q41': 'CompanySize', 'Q5': 'WebsiteTechnology', 'Q19': 'WebsiteStylesheet', 'Q2': 'SRIKnowledge',
                 'Q6': 'FirstAwareOFSRI', 'Q21': 'HowIncludeSRI', 'Q8': 'SRIandHTTPS', 'Q9': 'SRIandHTTP', 'Q16': 'DifferentAlg',
                 'Q26': 'SameAlgorithm', 'Q17': 'MalformedDigest', 'Q63': 'SRIExtended', 'Q59': 'WhichExtension',
                 'Q57': 'UseCredentialSnippet', 'Q58': 'AnonymousSnippet'}

file = open('output/survey.tex', 'w')

# Total respondents
total_respondents = (data.ResponseId.count())

file.write('\\def\\TotalRespondentsN{\\num{%d}\\xspace}\n' % total_respondents)

# Remove all invalid responses i.e., False
data = data[data.Finished != 'False']

for column in data:
    if column == 'ResponseId' or column == 'Finished':
        continue

    # Calculate the amount of each response
    d = dict(data.groupby(column).ResponseId.count())

    # Calculate the sum for the percentages
    sum_of_values = data.groupby(column).ResponseId.count().sum()

    # Transform Multiple choice columns
    if column == 'Q59' or column == 'Q21' or column == 'Q5':
        new_dict = {}
        for key in d:
            key_list = [x.strip() for x in key.split(',')]
            for val in key_list:
                # If this is a new input
                if val not in new_dict.keys():
                    new_dict[val] = d[key]
                # If this is an existing input
                else:
                    old_val = new_dict[val]
                    new_dict[val] = d[key] + old_val
        d = new_dict
        pass

    # Tranform the duration
    if column == 'Q6':
        new_dict = {}
        for key in d:
            if key == '< 1 month ago':
                new_dict['LessThanOneMonth'] = d[key]
                pass
            elif key == 'Between 3 months and 6 months ago':
                new_dict['BetweenThreeAndSixMonths'] = d[key]
                pass
            elif key == 'Between 6 months and 1 year ago':
                new_dict['BetweenSixMonthsAndAYear'] = d[key]
                pass
            elif key == 'Between 1 year and 4 years':
                new_dict['BetweenAYearAndFourYears'] = d[key]
                pass
            elif key == '> 4 years ago':
                new_dict['MoreThanFourYears'] = d[key]
                pass

        d = new_dict
        pass

    # Transform the age
    if column == 'Q14':
        new_dict = {}
        for key in d:
            if key == '18 - 24':
                new_dict['Btw Eighteen And Twenty Four'] = d[key]
                pass
            elif key == '25 - 34':
                new_dict['Btw Twenty Five And Thirty Four'] = d[key]
                pass
            elif key == '35 - 44':
                new_dict['Btw Thirty Five And Forty Four'] = d[key]
                pass
            elif key == '45 - 54':
                new_dict['Btw Forty Five And Fifty Four'] = d[key]
                pass
            elif key == '55 - 64':
                new_dict['Btw Fifty Five And Sixty Four'] = d[key]
                pass
            elif key == '65+':
                new_dict['Sixty Five Plus'] = d[key]
                pass

        # Compute the values for those more than 44 years old
        new_dict['Greater Than Forty Four'] = new_dict['Btw Forty Five And Fifty Four'] + \
            new_dict['Btw Fifty Five And Sixty Four'] + \
            new_dict['Sixty Five Plus']
        d = new_dict

    if column == 'Q15':
        d['unemployed'] = d['Not employed, and not looking for work'] + \
            d['Not employed, but looking for work']

    if column == 'Q41':
        d['Small Companies'] = d['Individually owned company'] + d['Startup']

    if column == 'Q40':
        d['Some Knowledge'] = d['Yes, I had some training sponsored by the company where I work'] + d['Yes, I have a University degree or equivalent in IT security '] + \
            d['Yes, I have a diploma in IT  security'] + d['Yes, I studied IT security in a college/university without earning a degree'] + \
            d['Yes, I took some courses in IT security but did not specialise in it']

    if column == 'Q2':
        d['Basics'] = d['Little knowledge (I heard the term)'] + \
            d['Some knowledge (I know the basics)']
        d['Part Of Development'] = d['Extensive knowledge (I use it often)'] + \
            d['Use knowledge (I used SRI)']

    for key in d:
        if "R_" in key:
            continue
        column_name = question_dict[column]
        clustered_key = key
        clustered_key = clustered_key.title()

        # Remove non-word characters
        pattern = re.compile('[\W_]+')
        clustered_key = pattern.sub('', clustered_key)

        clustered_key = column_name + clustered_key
        file.write('\\def\\%sN{\\num{%d}\\xspace}\n' % (clustered_key, d[key]))

        # Calculate the percentage
        percentage = (d[key]/sum_of_values) * 100
        file.write('\\def\\%sP{\\num{%.1f}\\%%\\xspace}\n' %
                   (clustered_key, percentage))

# Total valid (completed) responses
responses = dict(data.groupby('Finished').Finished.count())
total_valid_responses = responses['True']
file.write(
    '\\def\\TotalValidResponsesN{\\num{%d}\\xspace}\n' % total_valid_responses)

# Survival count
malformed = dict(data.groupby('Q17').ResponseId.count())
same_alg = dict(data.groupby('Q26').ResponseId.count())
diff_alg = dict(data.groupby('Q16').ResponseId.count())
sri_https = dict(data.groupby('Q8').ResponseId.count())

right_malformed = malformed['User-agent loads the resource in any case']
right_same_alg = same_alg['User-agent loads the resource only if any digest in the list matches that of the resource']
right_diff_alg = diff_alg['User-agent loads the resource only if the digest created with the strongest hashing algorithm matches that of the resource']
right_sri_https = sri_https['Yes']

# # Clean up Q21
# # Calculate the amount of each response
# d = dict(data.groupby('Q21').ResponseId.count())

# # Transform Multiple choice columns
# new_dict = {}
# for key in d:
#     key_list = [x.strip() for x in key.split(',')]
#     for val in key_list:
#         # If this is a new input
#         if val not in new_dict.keys():
#             new_dict[val] = d[key]
#         # If this is an existing input
#         else:
#             old_val = new_dict[val]
#             new_dict[val] = d[key] + old_val
# d = new_dict

# del d['Other (please specify):']


# # # Labels to use
# labels = {'I am not sure':'I am not\nsure',
#           'Compute the checksums of the subresources and include them myself':'Compute digests\nmyself',
#           'Configure my build tool to compute and include the checksums automatically':'Compute digests\nautomatically',
#           'Copy-paste snippets from online communities':'Copy-paste from\nonline communities',
#           'Copy-paste snippets from the official documentation':'Copy-paste from\nofficial documents'}

# new_dict = {}
# for key in labels:
#     new_key = labels[key]
#     new_dict[new_key] = d[key]
# d=new_dict

# # print(d)

# # Plotting Bar chart for RQ4 (Q21)
# d = pd.DataFrame(d.items(), columns=['Responses', 'Count'])
# ax = d.plot.bar(x='Responses',y='Count', rot='xticks', legend=False)

# ax.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)
# ax.yaxis.grid()

# ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])

# plt.xlabel('')
# plt.ylabel('')

# plt.xticks(rotation=45,  ha="right")

# plt.savefig('survey_incuding_sri.pdf', bbox_inches='tight')

# Pie Charts: global variables
color_idk = '#66b3ff'
color_correct = '#99ff99'
color_incorrect = '#ff9999'

# Used for translations QXX -> translations
t = {}

def plot_pie(values, translations, filename):
    l = sorted([translations[k] + (float(values[k]),) for k in values])
    _, labels, colors, counts = zip(*l)

    # explosion
    explode = (0.0,) * len(l)

    wedges, _, autotexts = plt.pie(counts, labels=labels, autopct='%1.1f%%',
                                   startangle=90, pctdistance=0.85, explode=explode, colors=colors)
    # wedges, _ = plt.pie(counts, labels=labels, autopct=None, startangle=90, pctdistance=0.85, explode=explode, colors=colors)
    centre_circle = plt.Circle((0, 0), 0.60, fc='white')

    for w in wedges:
        w.set_linewidth(2)
        w.set_edgecolor('white')

    for t in autotexts:
        t.set_fontsize(11)

    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)

    # Equal aspect ratio ensures that pie is drawn as a circle
    plt.axis('equal')
    plt.tight_layout()
    plt.savefig(filename, bbox_inches='tight')
    plt.clf()

# Pie Chart for SRIandHTTPS
d = data[data.Q8.notnull()]
print(d['Q8'].count())

d = dict(d.groupby('Q8').ResponseId.count())

# label -> (display order, short label, color)
t['Q8'] = {
    'I am not sure': (1, 'I am not sure', color_idk),
    'No': (2, 'No', color_incorrect),
    'Yes': (3, '\nYes', color_correct)
}

plot_pie(d, t['Q8'], 'output/survey_sri_and_https.pdf')
print(d)

# Pie Chart for diffAlgorithm
d = data[data.Q16.notnull()]
print(d['Q16'].count())

d = dict(d.groupby('Q16').ResponseId.count())

# label -> (display order, short label, color)
t['Q16'] = {'I am not sure': (1, 'I am not\nsure', color_idk),
            'User-agent loads the resource only if the digest created with the strongest hashing algorithm matches that of the resource': (2, '\n\nLoads only if the\nstrongest digest matches', color_correct),
            'User-agent loads the resource only if the first digest in the list matches that of the resource': (3, '\n\nLoads only if the\nfirst digest matches', color_incorrect),
            'User-agent does not load the resource at all': (4, 'Does not load at all', color_incorrect),
            'User-agent loads the resource in any case': (6, 'Loads in any case', color_incorrect),
            'User-agent loads the resource only if any digest in the list matches that of the resource': (5, 'Loads only if any\ndigest matches', color_incorrect),
            'Other (please specify) :': (4.5, 'Other', color_incorrect)
            }

plot_pie(d, t['Q16'], 'output/survey_diff_alg.pdf')
print(d)

# Pie Chart for sameAlgorithm
d = data[data.Q26.notnull()]
print(d['Q26'].count())

d = dict(d.groupby('Q26').ResponseId.count())

# Labels to use
t['Q26'] = {'I am not sure': (1, 'I am not\nsure', color_idk),
            'User-agent loads the resource only if all the digest in the list match': (2, '\n\nLoads only if all\nthe digest match', color_incorrect),
            'User-agent loads the resource only if the first digest in the list matches that of the resource': (3, '\n\nLoads only if the\nfirst digest matches', color_incorrect),
            'User-agent does not load the resource at all': (4, 'Does not load at all', color_incorrect),
            'User-agent loads the resource  in any case': (5, '\nLoads in any case', color_incorrect),
            'User-agent loads the resource only if any digest in the list matches that of the resource': (6, 'Loads only if any\ndigest matches', color_correct),
            'Other (please specify) :': (5.5, 'Other', color_incorrect)
            }

plot_pie(d, t['Q26'], 'output/survey_same_alg.pdf')
print(d)

# Pie Chart for malformed
d = data[data.Q17.notnull()]
print(d['Q17'].count())

d = dict(d.groupby('Q17').ResponseId.count())

# Labels to use
t['Q17'] = {
    'I am not sure': (1, 'I am not\nsure', color_idk),
    'User-agent does not load the resource at all': (3, '\nDoes not load at all', color_incorrect),
    'User-agent loads the resource in any case': (4, 'Loads in any case', color_correct),
    'Other (please specify) :': (2, 'Other', color_incorrect)
}

plot_pie(d, t['Q17'], 'output/survey_malformed.pdf')
print(d)

# import matplotlib.pyplot as plt

# plt.rc('font',**{'family':'serif','size':'11'})

# plt.rcParams['pdf.fonttype'] = 42
# plt.rcParams['ps.fonttype'] = 42

# # Pie chart
# labels = ['I am not\nsure', 'Loads only if the\nstrongest digest matches', 'Loads only if the\nfirst digest matches', 'Does not load at all',
#           'Loads only if any\ndigest matches', 'Loads in any case']
# sizes = [40, 6, 20, 2, 30, 2]
# # colors
# colors = ['#66b3ff', '#99ff99', '#ff9999', '#ff9999', '#ff9999', '#ff9999']
# # explosion
# explode = (0.02, 0.02, 0.02, 0.02, 0.02, 0.02)

# plt.pie(sizes, colors=colors, labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85, explode=explode)
# # draw circle
# centre_circle = plt.Circle((0, 0), 0.70, fc='white')
# fig = plt.gcf()
# fig.gca().add_artist(centre_circle)
# # Equal aspect ratio ensures that pie is drawn as a circle
# plt.axis('equal')
# plt.tight_layout()
# plt.show()
# fig.savefig('quiz_two_digests_different_algorithms.pdf')

quiz = ['Q8', 'Q16', 'Q26', 'Q17']
correct_answers = [
    ['Yes'],
    ['User-agent loads the resource only if the digest created with the strongest hashing algorithm matches that of the resource'],
    ['User-agent loads the resource only if any digest in the list matches that of the resource'],
    ['User-agent loads the resource in any case']
]

data_ = data
data_ = data_[quiz].dropna()

print(data_.count())
# for q in quiz:
#    data_= data_[data_[q] != 'Other (please specify) :']
# print(data_.head())
results = data_.apply(lambda l: list(
    map(lambda x, y: x in y, l, correct_answers)), axis=1, result_type='broadcast')

print(data['Q2'].value_counts())

d = dict(results.apply(lambda l: sum(
    [x for x in l if x]), axis=1).value_counts(normalize=True))
print(d)

keys = sorted(d.keys())

ax = plt.bar(keys, [d[k] for k in keys])

plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.gca().yaxis.grid()

plt.xlabel('num. of correct responses')
plt.ylabel('prop. of respondents')

loc, labels = plt.yticks()
plt.yticks(loc, ['{:,.0%}'.format(x) for x in plt.gca().get_yticks()])

plt.xticks(rotation='horizontal')

sm = plt.cm.ScalarMappable(norm=matplotlib.colors.Normalize(
    vmin=0, vmax=keys[-1], clip=True), cmap=matplotlib.colors.LinearSegmentedColormap.from_list('abc', ['#ff9999', '#fdfd96', '#99ff99']))

for i, p in enumerate(ax.patches):
    width, height = p.get_width(), p.get_height()
    x, y = p.get_xy()
    plt.annotate('{:2.1%}'.format(height), (x, y + height + 0.02))
    p.set_facecolor(sm.to_rgba(i))

plt.savefig('output/survey_survival.pdf', bbox_inches='tight')
plt.clf()

# Calculate and write the percentages in LaTeX macros
d_N = dict(results.apply(lambda l: sum(
    [x for x in l if x]), axis=1).value_counts(normalize=False))
d_P = dict(results.apply(lambda l: sum(
    [x for x in l if x]), axis=1).value_counts(normalize=True))

num2txt = {0: 'No', 1: 'One', 2: 'Two', 3: 'Three', 4: 'Four'}

for k in sorted(d_N.keys()):
    file.write('\\def\\Total%sCorrectResponse%sSRItoMalformedN{\\num{%d}\\xspace}\n' % (
        num2txt[k], 's' if k > 1 else '', d_N[k]))
    file.write('\\def\\Total%sCorrectResponse%sSRItoMalformedP{\\num{%.1f}\\%%\\xspace}\n' % (
        num2txt[k], 's' if k > 1 else '', 100*d_P[k]))

file.write(
    '\\def\\TotalResponsesSRItoMalformedN{\\num{%d}\\xspace}\n' % sum(d_N.values()))

# Same as above but only for people who use SRI

data_ = data[(data['Q2'] == 'Use knowledge (I used SRI)') | (
    data['Q2'] == 'Extensive knowledge (I use it often)')]
data_ = data_[quiz].dropna()
# for q in quiz:
#    data_= data_[data_[q] != 'Other (please specify) :']
# print(data_.head())
results = data_.apply(lambda l: list(
    map(lambda x, y: x in y, l, correct_answers)), axis=1, result_type='broadcast')

print(data_.count())

d = dict(results.apply(lambda l: sum(
    [x for x in l if x]), axis=1).value_counts(normalize=True))
print(d)

keys = sorted(d.keys())

ax = plt.bar(keys, [d[k] for k in keys])

plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.gca().yaxis.grid()

plt.xlabel('num. of correct responses')
plt.ylabel('prop. of respondents')

loc, labels = plt.yticks()
plt.yticks(loc, ['{:,.0%}'.format(x) for x in plt.gca().get_yticks()])

plt.xticks(rotation='horizontal')

sm = plt.cm.ScalarMappable(norm=matplotlib.colors.Normalize(
    vmin=0, vmax=keys[-1], clip=True), cmap=matplotlib.colors.LinearSegmentedColormap.from_list('abc', ['#ff9999', '#fdfd96', '#99ff99']))

for i, p in enumerate(ax.patches):
    width, height = p.get_width(), p.get_height()
    x, y = p.get_xy()
    plt.annotate('{:2.1%}'.format(height), (x, y + height + 0.02))
    p.set_facecolor(sm.to_rgba(i))

plt.savefig('output/survey_survival_use_SRI.pdf', bbox_inches='tight')
plt.clf()

file.close()
