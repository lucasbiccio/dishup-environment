package br.com.dishup.environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import org.hibernate.Session;
import org.hibernate.Transaction;
import br.com.dishup.core.entity.CountryEntity;
import br.com.dishup.core.exception.EmptyTableException;
import br.com.dishup.core.exception.FileEmptyException;
import br.com.dishup.core.persistence.HibernateUtil;
import br.com.dishup.core.persistence.CountryDAO;
import br.com.dishup.core.util.CountryComparatorUtil;
import br.com.dishup.core.util.StatisticFileUtil;

public class LoadCountryEnvironment {

	public void loadCountry(String filePath){
		File file = new File(filePath);
		StatisticFileUtil statistic = new StatisticFileUtil(new LoadCountryEnvironment(), "loadCountry(String filePath)", file, true, "dishup.pais");
		statistic.start();
		ArrayList<CountryEntity> listFile = new ArrayList<CountryEntity>();
		CountryDAO countryDAO = new CountryDAO();
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		try{
			listFile = loadFileIntoArray(file);
			Collections.sort(listFile, new CountryComparatorUtil());
			statistic.setNumberOfRegisterFile(listFile.size());
			List<CountryEntity> listBase = new ArrayList<CountryEntity>();;
			try {
				listBase = countryDAO.selectAllOrderById(session);
				statistic.setNumberOfRegisterBase(listBase.size());
				int countListBase = 0, countListFile = 0, numberOfRegisterBase = listBase.size(), numberOfRegisterFile = listFile.size();
				while((countListBase < numberOfRegisterBase) || (countListFile < numberOfRegisterFile)){
					if((countListBase < numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						if(listFile.get(countListFile).getId() == listBase.get(countListBase).getId()){
							countListBase++;
							countListFile++;
						}else if(listFile.get(countListFile).getId() > listBase.get(countListBase).getId()){
							countryDAO.deleteById(session,listBase.get(countListBase).getId());
							statistic.incrementRegisterDeleted();
							countListBase++;
						}else{
							countryDAO.insert(session,listFile.get(countListFile));
							statistic.incrementRegisterWrite();
							countListFile++;
						}
					}else if((countListBase < numberOfRegisterBase) && (countListFile >= numberOfRegisterFile)){
						countryDAO.deleteById(session,listBase.get(countListBase).getId());
						statistic.incrementRegisterDeleted();
						countListBase++;
					}else if((countListBase >= numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						countryDAO.insert(session,listFile.get(countListFile));
						statistic.incrementRegisterWrite();
						countListFile++;
					}
				}
			}catch (EmptyTableException e){
				statistic = new StatisticFileUtil(new LoadCountryEnvironment(), "loadCountry(String filePath)", file, false, "");
				statistic.start();
				statistic.setNumberOfRegisterFile(listFile.size());
				for(int i = 0; i < listFile.size(); i++){
					try{
						statistic.incrementRegisterRead();
						countryDAO.insert(session,listFile.get(i));
						statistic.incrementRegisterWrite();
					}catch(Exception e1){
						e1.printStackTrace();
					}
				}
			}
		}catch(Throwable e){
			e.printStackTrace();
		}
		transaction.commit();
		session.close();
		statistic.end();
		System.out.println(statistic.toString());
	}
	
	private ArrayList<CountryEntity> loadFileIntoArray(File file) throws FileEmptyException, FileNotFoundException{
		ArrayList<CountryEntity> listaPais = new ArrayList<CountryEntity>();
		Scanner scanner = new Scanner(file);
		String var = "";
		String[] parms;
		if(!scanner.hasNext())
			throw new FileEmptyException("FILE (PATH: "+file.getPath()+" ) IS EMPTY");
		while(scanner.hasNext()){
			try{
				var = scanner.nextLine();
				parms = var.split(";");
				listaPais.add(new CountryEntity(Integer.valueOf(parms[0]), parms[1], parms[2]));
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		scanner.close();
		return listaPais;
	}
}