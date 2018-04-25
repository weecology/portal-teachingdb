import os
import pandas as pd
import csv
import sqlalchemy

# Clean Surveys Table
surveys = pd.read_csv("http://esapubs.org/archive/ecol/E090/118/Portal_rodents_19772002.csv",
                      usecols=['recordID', 'mo', 'dy', 'yr', 'plot', 'species', 'sex', 'wgt', 'hfl'],
                      keep_default_na=False, na_values=[''])
surveys.rename(columns={'recordID': 'record_id', 'mo': 'month', 'dy': 'day',
                        'yr': 'year', 'plot': 'plot_id', 'species': 'species_id',
                        'wgt': 'weight', 'hfl': 'hindfoot_length'},
               inplace=True)
surveys.replace({'species_id': {'NA': 'NL'}, 'sex': {'P': float('nan'), 'R': float('nan'), 'Z': float('nan')}}, inplace=True)

# Replace invalid dates
surveys.loc[(surveys.year==2000) & (surveys.month==4) & (surveys.day==31),'day'] = 1
surveys.loc[(surveys.year==2000) & (surveys.month==4) & (surveys.day==1),'month'] = 5
surveys.loc[(surveys.year==2000) & (surveys.month==9) & (surveys.day==31), 'day'] = 1
surveys.loc[(surveys.year==2000) & (surveys.month==9) & (surveys.day==1), 'month'] = 10

# Clean Species Table
url = "https://ecologicaldata.org/sites/default/files/"
species = pd.read_csv(url + "portal_species.txt",
                      usecols=['New Code', 'ScientificName', 'Taxa'],
                      delimiter=';', keep_default_na=False,na_values=[''])
species.rename(columns={'New Code': 'species_id', 'Taxa': 'taxa'}, inplace=True)
species = species[species['species_id'] != 'XX'].reset_index()
species.replace({'species_id': {'NA': 'NL'}, 'taxa': {'Rodent-not censused': 'Rodent'}},
                inplace=True)
species_id = species['species_id']
taxa = species['taxa']
split_names = pd.DataFrame(species.ScientificName.str.split(' ').tolist(),
                           columns=['genus', 'species'])
species = pd.concat([species_id, split_names, taxa], axis=1)

# Clean Plots Table
plots = pd.read_csv(url + "portal_plots.txt",
                    names=['plot_id', 'plot_type_alpha', 'plot_type_num', 'plot_type'],
                    usecols=['plot_id', 'plot_type'])


# Create Combined Dataset
combined = pd.merge(surveys, species, on='species_id')
combined = pd.merge(combined, plots, on='plot_id')

# Export to csv
surveys.to_csv('surveys.csv', index=False, float_format='%i')
species.to_csv('species.csv', index=False)
plots.to_csv('plots.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
combined.to_csv('combined.csv', index=False, float_format='%i', quoting=csv.QUOTE_NONNUMERIC)


# Export to json

surveys.to_json('surveys.json', orient='records')
species.to_json('species.json', orient='records')
plots.to_json('plots.json', orient='records')
combined.to_json('combined.json', orient='records')

# Export to sqlite
class RoundedNumber(sqlalchemy.TypeDecorator):
    impl = sqlalchemy.types.Float

    def process_bind_param(self, value, dialect):
        if isinstance(value, float):
            return int(value + 0.5)
        return value


if os.path.isfile('portal_mammals.sqlite'):
    os.remove('portal_mammals.sqlite')
engine = sqlalchemy.create_engine('sqlite:///portal_mammals.sqlite')
surveys.to_sql('surveys', engine, index=False, dtype={'weight': RoundedNumber,
                                                      'hindfoot_length': RoundedNumber})
species.to_sql('species', engine, index=False)
plots.to_sql('plots', engine, index=False)
