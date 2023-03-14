import os
import time

class Screen():
    '''The Screen class provides methods for managing and displaying content on a screen.'''

    def wait(self,time_s):
        '''This method waits for the specified amount of time in seconds.'''

        time.sleep(time_s)

    def clean(self):
        '''This method clears the console screen.'''

        os.system('clear')

    def show_df(self, df_dict):
        '''This method prints the title of the dataframe and shows it.'''
        
        resposta = 'indefinida'
        while resposta != 'n' or resposta != 's':
            resposta = input("Os CSVs foram lidos e dataframes foram criados!\nDeseja vê-los? (s/n)\n")
            if resposta == 's':
                for key, value in df_dict.items():
                    print(key)
                    value.show()
                self.wait(3)
                resposta = 'indefinida'
                
            elif resposta == 'n':
                return 0
            else:
                print("Entrada inválida. Digite 's' para sim ou 'n' para não.")
    