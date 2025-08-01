from app.PostgresConnector import PostgresConnector
from app.Utils import Utils
import os
from app.logger_config import logger

class DataHandler:

    def read_file_list(self):
        try:
            with PostgresConnector() as db:
                return db.buscar("SELECT * FROM arquivos_novos;")
        except Exception as e:
            logger.error(f"Erro ao selecionar os dados: {e}", exc_info=True)            

    def verify_table(self,table_name):
        try:            
            with PostgresConnector() as db:
                exists = db.buscar(f"""SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name.lower()}'
                ); """)
            exists = exists[0][0]
            return exists            
        except Exception as e:            
            logger.error(f"Erro ao verificar existencia das tabelas: {e}", exc_info=True)

    def create_table(self,table,table_name):
        try:            
            create_table_query = ""
            
            if table_name.lower() == 'arquivos_erro':
                create_table_query = "CREATE TABLE public.arquivos_erro (nome text NULL);"
            elif table_name.lower() == 'arquivos_processados':
                create_table_query = "CREATE TABLE public.arquivos_processados (nome text NULL, hash text NULL, data date);"
            elif table_name.lower() == 'sinasc':
                create_table_query += "CREATE TABLE public.sinasc ( origem text NULL,codestab text NULL,	codmunnasc text NULL,	locnasc text NULL,	idademae text NULL,	estcivmae text NULL,	escmae text NULL,	codocupmae text NULL,	qtdfilvivo text NULL, "                
                create_table_query += "qtdfilmort text NULL,	codmunres text NULL,	gestacao text NULL,	gravidez text NULL,	parto text NULL,	consultas text NULL,	dtnasc text NULL,	horanasc text NULL,	sexo text NULL,	apgar1 text NULL,	apgar5 text NULL, "
                create_table_query += "racacor text NULL,	peso text NULL,	idanomal text NULL,	dtcadastro text NULL,	codanomal text NULL,	numerolote text NULL,	versaosist text NULL,	dtrecebim text NULL,	difdata text NULL,	dtrecoriga text NULL, "
                create_table_query += "naturalmae text NULL,	codmunnatu text NULL,	codufnatu text NULL,	escmae2010 text NULL,	seriescmae text NULL,	dtnascmae text NULL,	racacormae text NULL,	qtdgestant text NULL,	qtdpartnor text NULL,	qtdpartces text NULL,	idadepai text NULL,	dtultmenst text NULL,  "
                create_table_query += "semagestac text NULL,	tpmetestim text NULL,	consprenat text NULL,	mesprenat text NULL,	tpapresent text NULL,	sttrabpart text NULL,	stcesparto text NULL,	tpnascassi text NULL,	tpfuncresp text NULL, "
                create_table_query += "tpdocresp text NULL,	dtdeclarac text NULL,	escmaeagr1 text NULL,	stdnepidem text NULL,	stdnnova text NULL,	codpaisres text NULL,	tprobson text NULL,	paridade text NULL,	kotelchuck text NULL,	contador text NULL, codbainasc text NULL, codbaires text NULL, ufinform text NULL, "                
                create_table_query += "codcart text NULL, numregcart text NULL, codmuncart text NULL, dtregcart text NULL, dtrecorig text NULL, racacorn text NULL, racacor_rn text NULL, file_name TEXT, file_hash TEXT); "
            elif table_name.lower() == 'hant':
                create_table_query += "CREATE TABLE public.hant (tp_not text NULL, id_agravo text NULL, cs_suspeit text NULL, dt_notific text NULL, sem_not text NULL, nu_ano text NULL, "
                create_table_query += "sg_uf_not text NULL, id_municip text NULL, id_regiona text NULL, id_unidade text NULL, dt_sin_pri text NULL, sem_pri text NULL, dtdiasinac text NULL, "
                create_table_query += "ano_nasc text NULL, nu_idade_n text NULL, cs_sexo text NULL, cs_gestant text NULL, cs_raca text NULL, cs_escol_n text NULL, sg_uf text NULL, "
                create_table_query += "id_mn_resi text NULL, id_rg_resi text NULL, id_pais text NULL, dt_invest text NULL, id_ocupa_n text NULL, treina_mil text NULL, desmata_n text NULL, "
                create_table_query += "expo_n text NULL, moagem_n text NULL, dormiu_n text NULL, transpo_n text NULL, pescou_n text NULL, roedor_n text NULL, outra_ativ text NULL, "
                create_table_query += "outr_ati_d text NULL, cli_dt_ate text NULL, cli_febre text NULL, cli_tosse text NULL, cli_dispne text NULL, cli_respi text NULL, cli_cefale text NULL, "
                create_table_query += "cli_mial_g text NULL, cli_lombar text NULL, cli_abdomi text NULL, cli_hipote text NULL, cli_choque text NULL, cli_vomito text NULL, cli_diarre text NULL, "
                create_table_query += "cli_toraci text NULL, cli_tontur text NULL, cli_cardia text NULL, cli_renal text NULL, cli_neurol text NULL, cli_asteni text NULL, cli_petequ text NULL, "
                create_table_query += "cli_hemo text NULL, cli_h_desc text NULL, cli_outros text NULL, cli_out_d text NULL, am_sangue text NULL, lab_hema_n text NULL, lab_trombo text NULL, "
                create_table_query += "lab_atipic text NULL, lab_ureia text NULL, lab_tgo text NULL, lab_tgo_d text NULL, lab_tgp text NULL, lab_tgp_d text NULL, lab_res_b text NULL, "
                create_table_query += "lab_radiol text NULL, lab_difuso text NULL, lab_local text NULL, lab_derram text NULL, lab_colheu text NULL, dt_col_igm text NULL, lab_igm_r text NULL, "
                create_table_query += "lab_imuno text NULL, dt_rtpcr text NULL, lab_rtpcr text NULL, tra_hosp text NULL, tra_dt_int text NULL, tra_uf text NULL, tra_munici text NULL, "
                create_table_query += "tra_mecani text NULL, tra_antivi text NULL, tra_cortic text NULL, tra_cpap text NULL, tra_vasoat text NULL, tra_antibi text NULL, tra_tratam text NULL, "
                create_table_query += "tra_especi text NULL, classi_fin text NULL, con_forma text NULL, criterio text NULL, tpautocto text NULL, coufinf text NULL, copaisinf text NULL, "
                create_table_query += "comuninf text NULL, con_ambien text NULL, con_amb_de text NULL, con_locali text NULL, con_local2 text NULL, evolucao text NULL, dt_evoluc text NULL, "
                create_table_query += "con_autops text NULL, doenca_tra text NULL, dt_encerra text NULL, dt_digita text NULL, in_aids text NULL, cs_mening text NULL, dt_transus text NULL, "
                create_table_query += "dt_transdm text NULL, dt_transsm text NULL, dt_transrm text NULL, dt_trnasrs text NULL, dt_transse text NULL, nu_lote_v text NULL, nu_lote_h text NULL , file_name TEXT, file_hash TEXT);"
            elif table_name.lower() == 'trac':
                create_table_query += "CREATE TABLE public.trac (dt_notific text NULL, id_agravo text NULL, sem_not text NULL, nu_ano text NULL, sg_uf_not text NULL, id_municip text NULL, "
                create_table_query += "id_regiona text NULL, id_unidade text NULL, cs_inqueri text NULL, nu_caso text NULL, sg_uf text NULL, id_muni_re text NULL, cs_sexo text NULL, "
                create_table_query += "nu_idade_n text NULL, forma_tf text NULL, forma_ti text NULL, forma_ts text NULL, forma_tt text NULL, forma_co text NULL, encaminha text NULL, "
                create_table_query += "dt_digita text NULL, dt_transus text NULL, dt_transdm text NULL, dt_transsm text NULL, dt_transrm text NULL, dt_transrs text NULL, dt_transse text NULL, "
                create_table_query += "nu_lote_v text NULL, nu_lote_h text NULL, nu_notific text NULL, file_name TEXT, file_hash TEXT);"
            elif table_name.lower() == 'antr':
                create_table_query += "CREATE TABLE public.antr ( tp_not text NULL, id_agravo text NULL, dt_notific text NULL, sem_not text NULL, nu_ano text NULL, sg_uf_not text NULL, "
                create_table_query += "id_municip text NULL, id_regiona text NULL, id_unidade text NULL, dt_sin_pri text NULL, sem_pri text NULL, ano_nasc text NULL, nu_idade_n text NULL, "
                create_table_query += "cs_sexo text NULL, cs_gestant text NULL, cs_raca text NULL, cs_escol_n text NULL, sg_uf text NULL, id_mn_resi text NULL, id_rg_resi text NULL, "
                create_table_query += "id_pais text NULL, id_ocupa_n text NULL, ant_contat text NULL, ant_arranh text NULL, ant_lambed text NULL, ant_morded text NULL, ant_outro_ text NULL, "
                create_table_query += "ant_mucosa text NULL, ant_cabeca text NULL, ant_maos text NULL, ant_tronco text NULL, ant_membro text NULL, ant_memb_1 text NULL, ferimento text NULL, "
                create_table_query += "ant_profun text NULL, ant_superf text NULL, ant_dilace text NULL, ant_dt_exp text NULL, pre_expos text NULL, pos_expos text NULL, tra_antigo text NULL, "
                create_table_query += "doses text NULL, animal text NULL, herbiv_des text NULL, outro_des text NULL, condic_ani text NULL, observacao text NULL, trat_atual text NULL, "
                create_table_query += "lab_vacina text NULL, lab_vac_de text NULL, lote_vac text NULL, dt_vencim text NULL, dt_dose_1 text NULL, dt_dose_2 text NULL, dt_dose_3 text NULL, "
                create_table_query += "dt_dose_4 text NULL, dt_dose_5 text NULL, dt_vac_1 text NULL, dt_vac_2 text NULL, dt_vac_3 text NULL, dt_vac_4 text NULL, dt_vac_5 text NULL, "
                create_table_query += "fim_animal text NULL, tra_interr text NULL, tra_motivo text NULL, busca_ativ text NULL, reacao_vac text NULL, tra_indi_n text NULL, tra_peso text NULL, "
                create_table_query += "tra_qtd_so text NULL, tip_soro text NULL, tra_infilt text NULL, tra_infi_1 text NULL, lab_soro text NULL, lab_sor_de text NULL, tra_num_pa text NULL, "
                create_table_query += "reacao_sor text NULL, dt_encerra text NULL, dt_digita text NULL, id_cns_sus text NULL, dt_transus text NULL, file_name TEXT, file_hash TEXT);"
            elif table_name.lower() == 'zika':
                create_table_query += "CREATE TABLE public.zika (tp_not text NULL,id_agravo text NULL,dt_notific text NULL,sem_not text NULL,nu_ano text NULL,sg_uf_not text NULL, "
                create_table_query += "id_municip text NULL,id_regiona text NULL,id_unidade text NULL,dt_sin_pri text NULL,sem_pri text NULL,ano_nasc text NULL,nu_idade_n text NULL, "
                create_table_query += "cs_sexo text NULL,cs_gestant text NULL,cs_raca text NULL,cs_escol_n text NULL,sg_uf text NULL,id_mn_resi text NULL,id_rg_resi text NULL, "
                create_table_query += "id_pais text NULL,nduplic_n text NULL,in_vincula text NULL,dt_invest text NULL,id_ocupa_n text NULL,classi_fin text NULL,criterio text NULL, "
                create_table_query += "tpautocto text NULL,coufinf text NULL,copaisinf text NULL,comuninf text NULL,doenca_tra text NULL,evolucao text NULL,dt_obito text NULL, "
                create_table_query += "dt_encerra text NULL,dt_digita text NULL,tp_sistema text NULL,tpuninot text NULL,file_name text NULL,file_hash text NULL,cs_suspeit text NULL,"
                create_table_query += "cs_flxret text NULL,flxrecebi text NULL);"
            elif table_name.lower() == 'fmac':
                create_table_query += "CREATE TABLE public.fmac ( tp_not text NULL, id_agravo text NULL, dt_notific text NULL, sem_not text NULL, nu_ano text NULL, sg_uf_not text NULL, id_municip text NULL, "
                create_table_query += "id_regiona text NULL, id_unidade text NULL, dt_sin_pri text NULL, sem_pri text NULL, ano_nasc text NULL, nu_idade_n text NULL, cs_sexo text NULL, cs_gestant text NULL, "
                create_table_query += "cs_raca text NULL, cs_escol_n text NULL, sg_uf text NULL, id_mn_resi text NULL, id_rg_resi text NULL, id_pais text NULL, dt_invest text NULL, id_ocupa_n text NULL, febre text NULL, "
                create_table_query += "cefaleia text NULL, abdominal text NULL, mialgia text NULL, nausea text NULL, exantema text NULL, diarreia text NULL, ictericia text NULL, hiperemia text NULL, hepatome text NULL, "
                create_table_query += "petequias text NULL, hemorrag text NULL, linfadeno text NULL, convulsao text NULL, necrose text NULL, prostacao text NULL, choque text NULL, coma text NULL, hemorragi text NULL, "
                create_table_query += "respirato text NULL, oliguria text NULL, outros text NULL, outro_esp text NULL, carrapato text NULL, capivara text NULL, cao_gato text NULL, bovino text NULL, equinos text NULL, "
                create_table_query += "outroani text NULL, anim_esp text NULL, foi_mata text NULL, hospital text NULL, dtinterna text NULL, dtalta text NULL, coufhosp text NULL, diagno_lab text NULL, dts1 text NULL, "
                create_table_query += "dts2 text NULL, igm_s1 text NULL, tit_igm_s1 text NULL, igg_s1 text NULL, tit_igg_s1 text NULL, igm_s2 text NULL, tit_igm_s2 text NULL, igg_s2 text NULL, tit_igg_s2 text NULL, "
                create_table_query += "dt_coleta text NULL, dt_digita text NULL, isolamento text NULL, agente text NULL, histopato text NULL, imunohist text NULL, classi_fin text NULL, criterio text NULL, diag_desca text NULL, "
                create_table_query += "tpautocto text NULL, coufinf text NULL, copaisinf text NULL, comuninf text NULL, codisinf text NULL, zona text NULL, ambiente text NULL, doenca_tra text NULL, evolucao text NULL, "
                create_table_query += "dt_obito text NULL, dt_encerra text NULL, dt_transus text NULL, dt_transdm text NULL, dt_transrm text NULL, dt_transrs text NULL, dt_transse text NULL, nu_lote_v text NULL, "
                create_table_query += "nu_lote_h text NULL, ident_micr text NULL, file_name text NULL, file_hash text NULL, comunhosp text NULL);"
            elif table_name.lower() == 'deng':
                create_table_query += "CREATE TABLE public.deng (tp_not text NULL,id_agravo text NULL,dt_notific text NULL,sem_not text NULL,nu_ano text NULL,sg_uf_not text NULL,id_municip text NULL,id_regiona text NULL,"
                create_table_query += "id_unidade text NULL,dt_sin_pri text NULL,sem_pri text NULL,ano_nasc text NULL,nu_idade_n text NULL,cs_sexo text NULL,cs_gestant text NULL,cs_raca text NULL,cs_escol_n text NULL,"
                create_table_query += "sg_uf text NULL,id_mn_resi text NULL,id_rg_resi text NULL,id_pais text NULL,dt_invest text NULL,id_ocupa_n text NULL,febre text NULL,mialgia text NULL,cefaleia text NULL,exantema text NULL,"
                create_table_query += "vomito text NULL,nausea text NULL,dor_costas text NULL,conjuntvit text NULL,artrite text NULL,artralgia text NULL,petequia_n text NULL,leucopenia text NULL,laco text NULL,dor_retro text NULL,"
                create_table_query += "diabetes text NULL,hematolog text NULL,hepatopat text NULL,renal text NULL,hipertensa text NULL,acido_pept text NULL,auto_imune text NULL,dt_chik_s1 text NULL,dt_chik_s2 text NULL,dt_prnt text NULL,"
                create_table_query += "res_chiks1 text NULL,res_chiks2 text NULL,resul_prnt text NULL,dt_soro text NULL,resul_soro text NULL,dt_ns1 text NULL,resul_ns1 text NULL,dt_viral text NULL,resul_vi_n text NULL,dt_pcr text NULL,"
                create_table_query += "resul_pcr_ text NULL,sorotipo text NULL,histopa_n text NULL,imunoh_n text NULL,hospitaliz text NULL,dt_interna text NULL,uf text NULL,municipio text NULL,tpautocto text NULL,coufinf text NULL,"
                create_table_query += "copaisinf text NULL,comuninf text NULL,classi_fin text NULL,criterio text NULL,doenca_tra text NULL,clinc_chik text NULL,evolucao text NULL,dt_obito text NULL,dt_encerra text NULL,"
                create_table_query += "alrm_hipot text NULL,alrm_plaq text NULL,alrm_vom text NULL,alrm_sang text NULL,alrm_hemat text NULL,alrm_abdom text NULL,alrm_letar text NULL,alrm_hepat text NULL,alrm_liq text NULL,"
                create_table_query += "dt_alrm text NULL,grav_pulso text NULL,grav_conv text NULL,grav_ench text NULL,grav_insuf text NULL,grav_taqui text NULL,grav_extre text NULL,grav_hipot text NULL,grav_hemat text NULL,"
                create_table_query += "grav_melen text NULL,grav_metro text NULL,grav_sang text NULL,grav_ast text NULL,grav_mioc text NULL,grav_consc text NULL,grav_orgao text NULL,dt_grav text NULL,mani_hemor text NULL,epistaxe text NULL,"
                create_table_query += "gengivo text NULL,metro text NULL,petequias text NULL,hematura text NULL,sangram text NULL,laco_n text NULL,plasmatico text NULL,evidencia text NULL,plaq_menor text NULL,con_fhd text NULL,"
                create_table_query += "complica text NULL,tp_sistema text NULL,nduplic_n text NULL,dt_digita text NULL,cs_flxret text NULL,flxrecebi text NULL,migrado_w text NULL,file_name text NULL,file_hash text NULL,"
                create_table_query += "dt_nasc text NULL,nu_idade text NULL,id_dg_not text NULL,id_ev_not text NULL,ant_dt_inv text NULL,ocupacao text NULL,dengue text NULL,ano text NULL,vacinado text NULL,dt_dose text NULL,"
                create_table_query += "dt_febre text NULL,duracao text NULL,dor text NULL,prostacao text NULL,nauseas text NULL,diarreia text NULL,outros text NULL,sin_out text NULL,outros_m text NULL,outros_m_d text NULL,"
                create_table_query += "ascite text NULL,pleural text NULL,pericardi text NULL,abdominal text NULL,hepato text NULL,miocardi text NULL,hipotensao text NULL,choque text NULL,manifesta text NULL,insuficien text NULL,"
                create_table_query += "outro_s text NULL,outro_s_d text NULL,dt_choque text NULL,dt_col_hem text NULL,hema_maior text NULL,dt_col_plq text NULL,palq_maior text NULL,dt_col_he2 text NULL,hema_menor text NULL,dt_col_pl2 text NULL,"
                create_table_query += "dt_soro1 text NULL,dt_soro2 text NULL,dt_soror1 text NULL,dt_soror2 text NULL,s1_igm text NULL,s1_igg text NULL,s2_igm text NULL,s2_igg text NULL,s1_tit1 text NULL,s2_tit1 text NULL,"
                create_table_query += "material text NULL,soro1 text NULL,soro2 text NULL,tecidos text NULL,resul_vira text NULL,histopa text NULL,imunoh text NULL,amos_pcr text NULL,resul_pcr text NULL,amos_out text NULL,"
                create_table_query += "tecnica text NULL,resul_out text NULL,con_classi text NULL,con_criter text NULL,con_inf_mu text NULL,con_inf_uf text NULL,con_inf_pa text NULL,con_doenca text NULL,con_evoluc text NULL,con_dt_obi text NULL,"
                create_table_query += "con_dt_enc text NULL,in_vincula text NULL,nduplic text NULL,in_aids text NULL,cs_escolar text NULL);"
            elif table_name.lower() == 'chik':
                create_table_query += "CREATE TABLE public.chik (tp_not text NULL,id_agravo text NULL,dt_notific text NULL,sem_not text NULL,nu_ano text NULL,sg_uf_not text NULL,id_municip text NULL,"
                create_table_query += "id_regiona text NULL,id_unidade text NULL,dt_sin_pri text NULL,sem_pri text NULL,ano_nasc text NULL,nu_idade_n text NULL,cs_sexo text NULL,cs_gestant text NULL,cs_raca text NULL,"
                create_table_query += "cs_escol_n text NULL,sg_uf text NULL,id_mn_resi text NULL,id_rg_resi text NULL,id_pais text NULL,dt_invest text NULL,id_ocupa_n text NULL,febre text NULL,mialgia text NULL,"
                create_table_query += "cefaleia text NULL,exantema text NULL,vomito text NULL,nausea text NULL,dor_costas text NULL,conjuntvit text NULL,artrite text NULL,artralgia text NULL,petequia_n text NULL,leucopenia text NULL,"
                create_table_query += "laco text NULL,dor_retro text NULL,diabetes text NULL,hematolog text NULL,hepatopat text NULL,renal text NULL,hipertensa text NULL,acido_pept text NULL,auto_imune text NULL,dt_chik_s1 text NULL,"
                create_table_query += "dt_chik_s2 text NULL,dt_prnt text NULL,res_chiks1 text NULL,res_chiks2 text NULL,resul_prnt text NULL,dt_soro text NULL,resul_soro text NULL,dt_ns1 text NULL,resul_ns1 text NULL,dt_viral text NULL,"
                create_table_query += "resul_vi_n text NULL,dt_pcr text NULL,resul_pcr_ text NULL,sorotipo text NULL,histopa_n text NULL,imunoh_n text NULL,hospitaliz text NULL,dt_interna text NULL,uf text NULL,municipio text NULL,"
                create_table_query += "tpautocto text NULL,coufinf text NULL,copaisinf text NULL,comuninf text NULL,classi_fin text NULL,criterio text NULL,doenca_tra text NULL,clinc_chik text NULL,evolucao text NULL,"
                create_table_query += "dt_obito text NULL,dt_encerra text NULL,alrm_hipot text NULL,alrm_plaq text NULL,alrm_vom text NULL,alrm_sang text NULL,alrm_hemat text NULL,alrm_abdom text NULL,alrm_letar text NULL,"
                create_table_query += "alrm_hepat text NULL,alrm_liq text NULL,dt_alrm text NULL,grav_pulso text NULL,grav_conv text NULL,grav_ench text NULL,grav_insuf text NULL,grav_taqui text NULL,grav_extre text NULL,"
                create_table_query += "grav_hipot text NULL,grav_hemat text NULL,grav_melen text NULL,grav_metro text NULL,grav_sang text NULL,grav_ast text NULL,grav_mioc text NULL,grav_consc text NULL,grav_orgao text NULL,"
                create_table_query += "dt_grav text NULL,mani_hemor text NULL,epistaxe text NULL,gengivo text NULL,metro text NULL,petequias text NULL,hematura text NULL,sangram text NULL,laco_n text NULL,plasmatico text NULL,"
                create_table_query += "evidencia text NULL,plaq_menor text NULL,con_fhd text NULL,complica text NULL,nu_lote_i text NULL,tp_sistema text NULL,nduplic_n text NULL,dt_digita text NULL,cs_flxret text NULL,flxrecebi text NULL,"
                create_table_query += "migrado_w text NULL,file_name text NULL,file_hash text NULL,cs_suspeit text NULL,in_vincula text NULL,tpuninot text NULL);"
            else:
                field_names = table.columns.tolist()            
                # Criar a tabela dinamicamente
                create_table_query = f"CREATE TABLE {table_name} ("
                for field_name in field_names:                
                    create_table_query += f"{field_name} TEXT, "  # Def ault to TEXT for unknown types
                create_table_query += "file_name TEXT, file_hash TEXT, "  # Adiciona o nome do arquivo hash       
                create_table_query = create_table_query.replace("NATURAL", '"NATURAL"')         
                create_table_query = create_table_query.rstrip(', ') + ');'
            
            with PostgresConnector() as db:
                db.executar(create_table_query)            
        except Exception as e:            
            logger.error(f"Erro ao criar tabela: {e}", exc_info=True)

    def insert_table_data(self,records,table_name,file, file_hash):

        ## Preciso inserir qual a tabela e hash, para conseguir gerenciar qual o arquivo o reg pertence

        try:            
            # Get column names and data types from the DataFrame
            columns = ', '.join(records.columns)
            values_placeholder = ', '.join(['%s'] * len(records.columns))
            
            # Insert each row from the DataFrame into the table
            for index, row in records.iterrows():             
                insert_query = f"INSERT INTO {table_name} ({columns}, file_name, file_hash) VALUES ({values_placeholder}, '{file}', '{file_hash}')"
                insert_query = insert_query.replace("NATURAL", '"NATURAL"')

                with PostgresConnector() as db:
                    db.executar(insert_query, tuple(row))                
                    
        except Exception as e:
            logger.error(f"Erro ao inserir dados na tabela: {e}", exc_info=True)
            raise e

    def insert_to_processed_file(self, file,sha256_hash):
        try:                  
            file = Utils().change_extention(file,'.dbc')
            file = os.path.basename(file)
            
            comando = f"insert into arquivos_processados (nome,hash,data) values ('{file}','{sha256_hash}', now())"                        
            
            with PostgresConnector() as db:
                db.executar(comando)  
                    
        except Exception as e:
            logger.error(f"Erro insert into arquivos_processados: {e}", exc_info=True)

    def delete_local_file(self,file):
        try:
            os.remove(file)
        except Exception as e:
             logger.error(f"Erro ao deletar arquivos locais : {e}", exc_info=True)

    def file_with_error(self, file):
        try:
            comando = f"insert into arquivos_erro (nome) values('{file}')"

            with PostgresConnector() as db:
                db.executar(comando)              
        except Exception as e:            
            logger.error(f"Erro ao executar comando na função file_with_error : {e}", exc_info=True)

    def tabelas_iniciais(self):        
        if not self.verify_table('arquivos_erro'):
            self.create_table('','arquivos_erro')

        if not self.verify_table('arquivos_processados'):
            self.create_table('','arquivos_processados')

    def read_files_processed(self, file, hash=None):
        try:
            # Se o hash for fornecido, inclua-o na consulta
            if hash:
                comando = f"SELECT nome, hash, data FROM arquivos_processados WHERE nome = '{file}' AND hash = '{hash}'"
            else:
                comando = f"SELECT nome, hash, data FROM arquivos_processados WHERE nome = '{file}'"
    
            
            with PostgresConnector() as db:
                data = db.buscar(comando)
            # Mapeia os resultados para dicionários
            result = [{'nome': row[0], 'hash': row[1], 'data': row[2]} for row in data]
            return result
    
        except Exception as e:            
            logger.error(f"Erro ao selecionar os dados: {e}", exc_info=True)
            return None  # Retorna None ou uma lista vazia em caso de erro
    

    def insert_table_data_many(self, records, table_name, file, file_hash):
        try:
            # Colunas do DataFrame
            columns = ', '.join(records.columns)
            values_placeholder = ', '.join(['%s'] * len(records.columns))

            # Query de inserção
            insert_query = f"""
                INSERT INTO {table_name} ({columns}, file_name, file_hash)
                VALUES ({values_placeholder}, %s, %s)
            """

            # Converte DataFrame para lista de tuplas
            data = [tuple(row) + (file, file_hash) for row in records.to_numpy()]

            # Inserção em lote
            with PostgresConnector() as db:
                db.executar_many(insert_query, data)

        except Exception as e:
            logger.error(f"Erro ao inserir dados na tabela: {e}", exc_info=True)
            raise e