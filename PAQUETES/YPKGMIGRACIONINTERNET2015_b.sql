create or replace
PACKAGE BODY
/* $Header:: /PRODUCCION/PAQUETES/BODY/YPKGMIGRACIONINTERNET2015_b.sql 3 1.1.2 02.08.16 16:06 PSADM                     $ */
 YPKGMIGRACIONINTERNET2015
AS
  --Creado por Angel Salazar 11/05/2015  - PARA MIGRACION DE INTERNET PRY876
PROCEDURE PRD_PRINCIPAL(
    PV_NOM_ARCHIVO VARCHAR2,
    PV_OPCION      VARCHAR2,
    PV_USER        VARCHAR2,
    PV_PASSWORD    VARCHAR2)
IS
  vn_ctx NUMBER;
  F UTL_FILE.FILE_TYPE;
  V_LINE              VARCHAR2 (9000);
  vn_cparty_ida       VARCHAR2(50);
  vn_citema           VARCHAR2(50);
  vn_cparty_id        NUMBER;
  vn_account_id       NUMBER;
  vn_citem            NUMBER;
  vn_rootcitem_id     NUMBER;
  vn_verificapromo    NUMBER;
  vn_verificaregistro NUMBER;
  vn_verificacomando number;
BEGIN
  vn_ctx := ylogin (PV_USER, PV_PASSWORD);
  F      := UTL_FILE.FOPEN ('DOCS_FAC_ELEC', PV_NOM_ARCHIVO, 'R');
  IF UTL_FILE.IS_OPEN(F) THEN
    LOOP
      BEGIN
        UTL_FILE.GET_LINE(F, V_LINE);
        IF V_LINE IS NULL THEN
          EXIT;
        END IF;
        vn_cparty_ida :=REGEXP_SUBSTR(V_LINE, '[^,]+', 1, 1);
        vn_account_id := REGEXP_SUBSTR(V_LINE, '[^,]+', 1, 2);
        vn_citema     := REGEXP_SUBSTR(V_LINE, '[^,]+', 1, 3);
        SELECT REPLACE(REPLACE(REPLACE(REPLACE(vn_cparty_ida,CHR(10),' ') ,CHR(13),' ') ,chr(34),' '),' ',' ')
        INTO vn_cparty_id
        FROM dual;
        SELECT REPLACE(REPLACE(REPLACE(REPLACE(vn_citema,CHR(10),' ') ,CHR(13),' ') ,chr(34),' '),' ',' ')
        INTO vn_citem
        FROM dual;
        DBMS_OUTPUT.PUT_LINE('cparty: '||vn_cparty_id);
        DBMS_OUTPUT.PUT_LINE('account: '||vn_account_id);
        DBMS_OUTPUT.PUT_LINE('citem: '||vn_citem);
        vn_verificaregistro:=FNVERIFICA_EXISTENCIA(vn_cparty_id,vn_account_id,vn_citem);
        --Verifica Existencia
        IF vn_verificaregistro=1 THEN
          DBMS_OUTPUT.PUT_LINE('Paso verificacion existencia');
          --verificacion de promocion
          vn_verificapromo  :=FNVERIFICA_PROMO(vn_cparty_id,vn_account_id,vn_citem);
          IF vn_verificapromo=1 THEN
            DBMS_OUTPUT.PUT_LINE('Paso verificacion promocion');
            SELECT rootcitem_id
                INTO vn_rootcitem_id
                FROM TAMCONTRACTEDITEMD
                WHERE citem_id      =vn_citem; --obtiene rootcitem
            CASE PV_OPCION
            WHEN 'M' THEN
              DBMS_OUTPUT.PUT_LINE('Masivo');
              BEGIN
                YPKGINTERNET.RRESETSPEED(vn_cparty_id, vn_account_id); --Modifico la tabla YTINTERNETSPEED
                commit;
                DBMS_OUTPUT.PUT_LINE('pasoo YPKGINTERNET.RRESETSPEED');
              EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK;
                DBMS_OUTPUT.PUT_LINE('NO VALIO YPKGINTERNET.RRESETSPEED');
                INSERT
                INTO Yt_MigracionInt2015_log VALUES
                  (
                    vn_cparty_id,
                    vn_account_id,
                    vn_rootcitem_id,
                    sysdate,
                    'Error RRESETSPEED'
                  );
                  commit;
              END;
            WHEN 'I' THEN
              DBMS_OUTPUT.PUT_LINE('Individual');
              BEGIN
                BEGIN
                  YPKGINTERNET.RRESETSPEED(vn_cparty_id, vn_account_id);
                  commit;
                  DBMS_OUTPUT.PUT_LINE('YPKGINTERNET.RRESETSPEED');
                EXCEPTION
                WHEN OTHERS THEN
                  ROLLBACK;
                  INSERT
                  INTO Yt_MigracionInt2015_log VALUES
                    (
                      vn_cparty_id,
                      vn_account_id,
                      vn_rootcitem_id,
                      sysdate,
                      'Error RRESETSPEED'
                    );
                    commit;
                END;
                vn_verificacomando:=0;
                DBMS_OUTPUT.PUT_LINE('XPSPENGINE.pGenerateSPActions');
                XPSPENGINE.pGenerateSPActions(pi_citem_id => vn_rootcitem_id 
                ,pi_cparty_id => vn_cparty_id 
                ,pv_activity_type => 'R'
                ,pv_for_all => 'N' 
                ,pv_usejob => 'Y' 
                ,PV_TARGET => 'A' 
                ,PI_WFPROCESS_ID => NULL );
                commit;
                while vn_verificacomando=0 
                loop
                  vn_verificacomando:=FNVERIFICA_COMANDO(vn_cparty_id,vn_rootcitem_id);
                  DBMS_OUTPUT.PUT_LINE('Verifica Comando:'||vn_verificacomando);
                end loop;
              EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK;
                INSERT
                INTO Yt_MigracionInt2015_log VALUES
                  (
                    vn_cparty_id,
                    vn_account_id,
                    vn_rootcitem_id,
                    sysdate,
                    'Error pGenerateSPActions'
                  );
                  commit;
                --insert into Yt_newspeedlog values(vn_cparty_id, vn_account_id, 'Error pGenerateSPActions');
              END;
            END CASE;
          ELSE
            DBMS_OUTPUT.PUT_LINE('insert log prommo');
            INSERT
            INTO Yt_MigracionInt2015_log VALUES
              (
                vn_cparty_id,
                vn_account_id,
                vn_rootcitem_id,
                sysdate,
                'Cliente tiene promocion de upgrade activa'
              );
              commit;
          END IF;
        ELSE
          DBMS_OUTPUT.PUT_LINE('insert log existencia');
          INSERT
          INTO Yt_MigracionInt2015_log VALUES
            (
              vn_cparty_id,
              vn_account_id,
              vn_rootcitem_id,
              sysdate,
              'El registro no existe'
            );
            commit;
        END IF;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        EXIT;
      END;
    END LOOP;
  END IF;
  commit;
  UTL_FILE.FCLOSE(F);
END PRD_PRINCIPAL;
--Funcion para caducar upgrades PRY_952
PROCEDURE YPRD_MIGRAR_INT(
    PV_NOM_ARCHIVO      VARCHAR2,
    PV_OPCION           VARCHAR2,
    PV_USER             VARCHAR2,
    PV_PASSWORD         VARCHAR2
)
IS
    vn_ctx              NUMBER;
    F UTL_FILE.FILE_TYPE;
    V_LINE              VARCHAR2 (9000);
    vn_cparty_ida       VARCHAR2 (50);
    vn_citema           VARCHAR2 (50);
    vn_cparty_id        NUMBER;
    vn_account_id       NUMBER;
    vn_citem            NUMBER;
    vn_rootcitem_id     NUMBER;
    vn_verificapromo    NUMBER;
    vn_verificaregistro NUMBER;
    vn_verificacomando  NUMBER;
    --TEMPORALES
    VI_CONTPRO          INTEGER:=0;
    VI_CONTSAT          INTEGER:=0;
    VI_CONTLEXI         INTEGER:=0;
    VI_CONTLPRO         INTEGER:=0;
    
BEGIN
    vn_ctx := ylogin (PV_USER, PV_PASSWORD);
    F      := UTL_FILE.FOPEN ('DOCS_FAC_ELEC', PV_NOM_ARCHIVO, 'R'); --DOCS_FAC_ELEC
	
	IF UTL_FILE.IS_OPEN(F) THEN
		LOOP
			BEGIN
				UTL_FILE.GET_LINE(F, V_LINE);
				
				IF V_LINE IS NULL THEN
					EXIT;
				END IF;
				
				vn_cparty_ida := REGEXP_SUBSTR(V_LINE, '[^,]+', 1, 1);
				vn_account_id := REGEXP_SUBSTR(V_LINE, '[^,]+', 1, 2);
				vn_citema     := REGEXP_SUBSTR(V_LINE, '[^,]+', 1, 3);
				
				SELECT REPLACE(REPLACE(REPLACE(REPLACE(vn_cparty_ida,CHR(10),' ') ,CHR(13),' ') ,chr(34),' '),' ',' ')
					INTO vn_cparty_id
				FROM dual;
				
				SELECT REPLACE(REPLACE(REPLACE(REPLACE(vn_citema,CHR(10),' ') ,CHR(13),' ') ,chr(34),' '),' ',' ')
					INTO vn_citem
				FROM dual;
        
				DBMS_OUTPUT.PUT_LINE('********************************************************************');
				DBMS_OUTPUT.PUT_LINE('cparty: '  || vn_cparty_id);
				DBMS_OUTPUT.PUT_LINE('account: ' || vn_account_id);
				DBMS_OUTPUT.PUT_LINE('citem: '   || vn_citem);

				vn_verificaregistro := FNVERIFICA_EXISTENCIA(vn_cparty_id, vn_account_id, vn_citem);
				--Verifica Existencia
				IF vn_verificaregistro = 1 THEN
					DBMS_OUTPUT.PUT_LINE('Paso verificacion existencia');
					--verificacion de promocion
					vn_verificapromo  := YFNVERIFICA_UPGRADE(vn_cparty_id, vn_account_id, vn_citem);
					
					IF vn_verificapromo > 0 THEN
						DBMS_OUTPUT.PUT_LINE('Paso verificacion promocion');
						SELECT rootcitem_id
							INTO vn_rootcitem_id
						FROM TAMCONTRACTEDITEMD
							WHERE citem_id = vn_citem; --obtiene rootcitem
              
            IF vn_verificapromo = 1 THEN  
               UPDATE YTPROMOCIONESLOG SET ISFINISHED = 'Y', FINISHDATE = TRUNC(SYSDATE), 
                  ISSUSPENDED = 'Y', DATESUSPENDED =  trunc(sysdate)   
                WHERE  ACCOUNTID = vn_account_id
                  AND  CPARTYID  = vn_cparty_id AND CITEMID = vn_citem;
                  
               VI_CONTPRO := VI_CONTPRO + 1;
            END IF;
					
						CASE PV_OPCION
							WHEN 'M' THEN
								DBMS_OUTPUT.PUT_LINE('Masivo');
								BEGIN
                  vn_ctx := ylogin (PV_USER, PV_PASSWORD);
                  
									YPKGINTERNET.RRESETSPEED(vn_cparty_id, vn_account_id); --Modifico la tabla YTINTERNETSPEED
                  COMMIT;
									DBMS_OUTPUT.PUT_LINE('PASÓ YPKGINTERNET.RRESETSPEED');
                             
                  VI_CONTSAT := VI_CONTSAT + 1;
									EXCEPTION
										WHEN OTHERS THEN
										ROLLBACK;
									
										DBMS_OUTPUT.PUT_LINE('FALLÓ EN YPKGINTERNET.RRESETSPEED');
										INSERT
											INTO Yt_MigracionInt2015_log VALUES
											(
                          vn_cparty_id,
                          vn_account_id,
                          vn_rootcitem_id,
                          sysdate,
                          'Error RRESETSPEED'
											);
										COMMIT;
								END;
							WHEN 'I' THEN
								DBMS_OUTPUT.PUT_LINE('Individual');
								BEGIN
									BEGIN
                    vn_ctx := ylogin (PV_USER, PV_PASSWORD);
                    
										YPKGINTERNET.RRESETSPEED(vn_cparty_id, vn_account_id); --Modifico la tabla YTINTERNETSPEED
										COMMIT;   
                    
										DBMS_OUTPUT.PUT_LINE('PASÓ YPKGINTERNET.RRESETSPEED');
                    
										EXCEPTION
											WHEN OTHERS THEN
											ROLLBACK;
											DBMS_OUTPUT.PUT_LINE('FALLÓ EN YPKGINTERNET.RRESETSPEED');
											INSERT
												INTO Yt_MigracionInt2015_log VALUES
												(
                            vn_cparty_id,
                            vn_account_id,
                            vn_rootcitem_id,
                            sysdate,
                            'Error RRESETSPEED'
												);
											COMMIT;
									END;
									vn_verificacomando := 0;
									
									XPSPENGINE.pGenerateSPActions(
                      pi_citem_id       => vn_rootcitem_id 
                      ,pi_cparty_id     => vn_cparty_id 
                      ,pv_activity_type => 'R'
                      ,pv_for_all       => 'N' 
                      ,pv_usejob        => 'Y' 
                      ,PV_TARGET        => 'A' 
                      ,PI_WFPROCESS_ID  => NULL 
									);
									COMMIT;
									DBMS_OUTPUT.PUT_LINE('PASÓ XPSPENGINE.pGenerateSPActions');
                  VI_CONTSAT := VI_CONTSAT + 1;
									
									WHILE vn_verificacomando = 0 
									LOOP
										vn_verificacomando := FNVERIFICA_COMANDO(vn_cparty_id, vn_rootcitem_id);
										DBMS_OUTPUT.PUT_LINE('Verifica Comando:' || vn_verificacomando);
									END LOOP;
									
									EXCEPTION
										WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('FALLÓ XPSPENGINE.pGenerateSPActions');
										ROLLBACK;
										
										INSERT
											INTO Yt_MigracionInt2015_log VALUES
											(
                          vn_cparty_id,
                          vn_account_id,
                          vn_rootcitem_id,
                          sysdate,
                          'Error pGenerateSPActions'
											);
										COMMIT;
                    VI_CONTLPRO := VI_CONTLPRO + 1;
								END;
						END CASE;
					ELSE
						DBMS_OUTPUT.PUT_LINE('insert log prommo');
						INSERT
							INTO Yt_MigracionInt2015_log VALUES
							(
                  vn_cparty_id,
                  vn_account_id,
                  vn_rootcitem_id,
                  SYSDATE,
                  'Cliente no puede ser migrado'
							);
						COMMIT;
            VI_CONTLPRO := VI_CONTLPRO + 1;
					END IF;
				ELSE
					DBMS_OUTPUT.PUT_LINE('insert log existencia');
					INSERT
						INTO Yt_MigracionInt2015_log VALUES
						(
                vn_cparty_id,
                vn_account_id,
                vn_rootcitem_id,
                sysdate,
                'El registro no existe'
						);
					COMMIT;          
          VI_CONTLEXI := VI_CONTLEXI + 1;
				END IF;
				EXCEPTION
					WHEN NO_DATA_FOUND THEN
					EXIT;
			END;
		END LOOP;
     
	UTL_FILE.FCLOSE(F);
	END IF;
  
  DBMS_OUTPUT.PUT_LINE('/************ RESUMEN  *****************/');
  DBMS_OUTPUT.PUT_LINE('VI_CONTPRO: '|| VI_CONTPRO);
  DBMS_OUTPUT.PUT_LINE('VI_CONTSAT: '|| VI_CONTSAT);
  DBMS_OUTPUT.PUT_LINE('VI_CONTLEXI: '|| VI_CONTLEXI);
  DBMS_OUTPUT.PUT_LINE('VI_CONTLPRO: '|| VI_CONTLPRO);
     
	COMMIT;
 
END YPRD_MIGRAR_INT;
--fin caducar promociones PRY_952

--PROCEDIMIENTO CADUCA UPGRADE PRY_952
PROCEDURE YPRD_CADUCA_UPGRADE(
		pi_cparty_id        IN NUMBER,
		pi_cpartyaccount_id IN NUMBER,
		pi_citem_id         IN NUMBER)
IS

BEGIN
    UPDATE YTPROMOCIONESLOG SET ISFINISHED  = 'Y', FINISHDATE = TRUNC(SYSDATE), 
         ISSUSPENDED   = 'Y', DATESUSPENDED = trunc(sysdate)   
      WHERE  ACCOUNTID = pi_cpartyaccount_id
         AND CPARTYID  = pi_cparty_id AND CITEMID = pi_citem_id;
    COMMIT; 
    
    EXCEPTION WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('UPDATE FALLIDO: ' || PI_CPARTYACCOUNT_ID);	
      ROLLBACK;
END YPRD_CADUCA_UPGRADE;
--FIN PROCEDIMIENTO CADUCA UPGRADE
--Funcion para verificar existencia de cliente,cuenta,citem
FUNCTION FNVERIFICA_EXISTENCIA
  (
    pi_cparty_id        IN NUMBER,
    pi_cpartyaccount_id IN NUMBER,
    pi_citem_id         IN NUMBER
  )
  RETURN NUMBER
IS
  VN_INDICADOR NUMBER;
  vn_auxiliar  NUMBER;
BEGIN
  SELECT 1
  INTO vn_auxiliar
  FROM tamcontracteditemd tc
  WHERE tc.cparty_id     =pi_cparty_id
  AND tc.cpartyaccount_id=pi_cpartyaccount_id
  AND tc.citem_id        =pi_citem_id
  AND tc.contractedto   IS NULL
  AND tc.state          IN ('A','S','O')
  AND tc.isvalid         ='Y'
  AND tc.tariffplan_id   =34
  AND tc.product_id      =106;
  IF vn_auxiliar         =1 THEN
    vn_indicador        :=1;
  END IF;
  RETURN vn_indicador;
EXCEPTION
WHEN NO_DATA_FOUND THEN
  vn_indicador:=0;
  RETURN VN_INDICADOR;--No existe registro
END FNVERIFICA_EXISTENCIA;
--Funcion para verificar si no tiene promocion de upgrade
FUNCTION FNVERIFICA_PROMO(
    pi_cparty_id        IN NUMBER,
    pi_cpartyaccount_id IN NUMBER,
    pi_citem_id         IN NUMBER)
  RETURN NUMBER
IS
  VN_INDICADOR NUMBER;
  vn_auxiliar  NUMBER;
BEGIN
  SELECT 1
  INTO vn_auxiliar
  FROM tamcontracteditemd tc
  WHERE tc.cparty_id     =pi_cparty_id
  AND tc.cpartyaccount_id=pi_cpartyaccount_id
  AND tc.citem_id        =pi_citem_id
  AND tc.contractedto   IS NULL
  AND tc.state           ='A'
  AND tc.isvalid         ='Y'
  AND tc.tariffplan_id   =34
  AND tc.product_id      =106
  AND EXISTS
    (SELECT 1
    FROM ytpromocioneslog yp
    WHERE yp.cpartyid   =tc.cparty_id
    AND yp.accountid    =tc.cpartyaccount_id
    AND yp.citemid      =tc.citem_id
    AND yp.isfinished  IS NULL
    AND yp.issuspended IS NULL
    AND sysdate BETWEEN NVL (yp.datefrom, sysdate) AND NVL (yp.dateto, sysdate)
    AND yp.tariffplan_id=34
    AND yp.dateto      IS NOT NULL
    AND yp.issuspended IS NULL
    );
  DBMS_OUTPUT.PUT_LINE('indicador: '||VN_INDICADOR);
  IF vn_auxiliar =1 THEN
    vn_indicador:=0;
  END IF;
  RETURN VN_INDICADOR; -- tiene promocion
EXCEPTION
WHEN NO_DATA_FOUND THEN
  vn_indicador:=1;
  RETURN VN_INDICADOR;-- no tiene promocion
END FNVERIFICA_PROMO;
--Procedimiento para verificar estado de comando
FUNCTION FNVERIFICA_COMANDO(
    PN_CPARTY_ID IN NUMBER,
    PN_ROOTCITEM_ID IN NUMBER)
RETURN NUMBER
is
   Vv_estado varchar2(3);
   vn_retorna number;
begin
  SELECT   q.state into Vv_estado
    FROM tspactionqueue q, ttx x, tumusers u, tsessions s, tspactiondefs d 
   WHERE 1 = 1 
   and x.session_id = s.id
   AND Q.CPARTY_ID =  PN_CPARTY_ID
   and q.citem_id=PN_ROOTCITEM_ID
   AND q.REQUESTEDDATE >= SYSDATE
   and q.ctx_id = x.id
   and x.user_id = u.id
   and q.actiondef_id = d.id
  ORDER BY Q.ID DESC;
  DBMS_OUTPUT.PUT_LINE('estado: '||vv_estado);
  if vv_estado <> 'Q' then
    vn_retorna:=1;
  else
    vn_retorna:=0;
  end if;
  return vn_retorna;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vn_retorna:=2;
    return vn_retorna; --Si no se encuentran datos del comando
  end FNVERIFICA_COMANDO;  
 --FUHNCION VERIFICA UPGRADES

FUNCTION YFNVERIFICA_UPGRADE(
	PI_CPARTY_ID        IN NUMBER,
	PI_CPARTYACCOUNT_ID IN NUMBER,
	PI_CITEM_ID         IN NUMBER
)
RETURN NUMBER
IS
	VN_INDICADOR             NUMBER  := 2;
	VI_COSTCENTER_ID         INTEGER := 0;
	VI_COSTCENTER_ID2        INTEGER := 0;
	VI_TARIFFPLANVARIANT_ID  INTEGER := 0;
	VI_TARIFFPLANVARIANT_ID2 INTEGER := 0;

BEGIN
	SELECT COUNT(*) 
		INTO VI_COSTCENTER_ID 
	FROM TAMCPARTYACCOUNTD 
		WHERE ACCOUNT_ID = pi_cpartyaccount_id AND COSTCENTER_ID = 339;
BEGIN
	IF VI_COSTCENTER_ID > 0 THEN
		DBMS_OUTPUT.PUT_LINE('CUENTA ES CABLE FUTURO: ' || VI_COSTCENTER_ID);
		VN_INDICADOR := -1;
	ELSE		
		SELECT DISTINCT YP.TARIFFPLANVARIANT_ID, TA.COSTCENTER_ID
			INTO VI_TARIFFPLANVARIANT_ID, VI_COSTCENTER_ID 
		FROM YTPROMOCIONESLOG YP, YTINTERNETSPEED SS, TAMCPARTYACCOUNTD TA 
			WHERE YP.ACCOUNTID 		    = PI_CPARTYACCOUNT_ID
				AND YP.CPARTYID		      = PI_CPARTY_ID
				AND YP.CITEMID		      = PI_CITEM_ID            
				AND SS.CPARTY_ID        = YP.CPARTYID 
				AND SS.CPARTYACCOUNT_ID = YP.ACCOUNTID 
				AND SS.CITEM_ID         = YP.CITEMID
				AND SS.CPARTYACCOUNT_ID = TA.ACCOUNT_ID
				AND YP.TARIFFPLAN_ID    = 34
				AND YP.ISFINISHED       IS NULL
				AND YP.FINISHDATE       IS NULL
				AND SS.DTO              IS NULL
				AND TA.COSTCENTER_ID    IS NOT NULL
				AND SYSDATE BETWEEN  NVL (YP.DATEFROM, SYSDATE) AND NVL (YP.DATETO, SYSDATE);           
			
		IF(VI_TARIFFPLANVARIANT_ID = 923 AND (VI_COSTCENTER_ID = 12 OR VI_COSTCENTER_ID = 14)) THEN  
			DBMS_OUTPUT.PUT_LINE('CUENTA DE LOJA O CUENCA Y PLAN BASICO (UPGRADE): ' || PI_CPARTYACCOUNT_ID);
			VN_INDICADOR := 0;
		ELSE
			DBMS_OUTPUT.PUT_LINE('TIENE UPGRADE Y SE MIGRA: ' || PI_CPARTYACCOUNT_ID || VI_TARIFFPLANVARIANT_ID);
      VN_INDICADOR := 1;   
    END IF;
	END IF;	 
  
  EXCEPTION WHEN NO_DATA_FOUND THEN  
      DBMS_OUTPUT.PUT_LINE('NO POSEE PROMO DE UPGRADE: ' || PI_CPARTYACCOUNT_ID || VI_TARIFFPLANVARIANT_ID);	
      BEGIN
        SELECT DISTINCT TO_NUMBER(TM.TARIFFPLANVARIANT_ID), TO_NUMBER(TA.COSTCENTER_ID)
          INTO VI_TARIFFPLANVARIANT_ID, VI_COSTCENTER_ID
        FROM YTINTERNETSPEED SS, TAMCPARTYACCOUNTD TA, TAMCONTRACTEDITEMD TM 
          WHERE SS.CPARTY_ID        = PI_CPARTY_ID
            AND SS.CPARTYACCOUNT_ID = PI_CPARTYACCOUNT_ID
            AND SS.CITEM_ID         = PI_CITEM_ID
            AND SS.CPARTYACCOUNT_ID = TA.ACCOUNT_ID
            AND SS.CPARTY_ID        = TM.CPARTY_ID
            AND SS.CPARTYACCOUNT_ID = TM.CPARTYACCOUNT_ID
            AND SS.CITEM_ID         = TM.CITEM_ID
            AND SS.DTO              IS NULL
            AND TA.COSTCENTER_ID    IS NOT NULL;      
                    
        IF(VI_TARIFFPLANVARIANT_ID = 923 AND (VI_COSTCENTER_ID = 12 OR VI_COSTCENTER_ID = 14)) THEN  
          DBMS_OUTPUT.PUT_LINE('CUENTA DE LOJA O CUENCA Y PLAN BASICO: ' || PI_CPARTYACCOUNT_ID || VI_TARIFFPLANVARIANT_ID);
          VN_INDICADOR := 0;
        ELSE
          DBMS_OUTPUT.PUT_LINE('SE MIGRA: ' || PI_CPARTYACCOUNT_ID);
          VN_INDICADOR := 2;   
        END IF;
        
        EXCEPTION WHEN NO_DATA_FOUND THEN  
          DBMS_OUTPUT.PUT_LINE('NO SE PUEDE PROCESAR: ' || PI_CPARTYACCOUNT_ID);
          VN_INDICADOR := -2; 
          
      END;
  END; 
  
	DBMS_OUTPUT.PUT_LINE('VN_INDICADOR: ' || VN_INDICADOR);	
	RETURN VN_INDICADOR;
END YFNVERIFICA_UPGRADE;
-- FIN FUNCION VERIFICA UPGRADES 
END YPKGMIGRACIONINTERNET2015;