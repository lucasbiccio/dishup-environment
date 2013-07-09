package br.com.dishup.environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import org.hibernate.Session;
import org.hibernate.Transaction;
import br.com.dishup.core.entity.CityEntity;
import br.com.dishup.core.exception.EmptyTableException;
import br.com.dishup.core.exception.FileEmptyException;
import br.com.dishup.core.persistence.CityDAO;
import br.com.dishup.core.persistence.StateDAO;
import br.com.dishup.core.persistence.HibernateUtil;
import br.com.dishup.core.persistence.CountryDAO;
import br.com.dishup.core.util.CityComparatorUtil;
import br.com.dishup.core.util.StatisticFileUtil;

public class LoadCityEnvironment {
	
	public void loadCity(String filePath){
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		
		File file = new File(filePath);
		
		StatisticFileUtil statistic = new StatisticFileUtil(new LoadCityEnvironment(), "loadCity(String filePath)", file, true, "dishup.cidade");
		statistic.start();
		
		ArrayList<CityEntity> listFile = new ArrayList<CityEntity>();
		CityDAO cityDAO = new CityDAO();
		
		try{
			listFile = loadFileIntoArray(file);
			Collections.sort(listFile, new CityComparatorUtil());
			statistic.setNumberOfRegisterFile(listFile.size());
			List<CityEntity> listBase = new ArrayList<CityEntity>();;
			try {
				listBase = cityDAO.selectAllOrderById(session);
				statistic.setNumberOfRegisterBase(listBase.size());
				int countListBase = 0, countListFile = 0, numberOfRegisterBase = listBase.size(), numberOfRegisterFile = listFile.size();
				while((countListBase < numberOfRegisterBase) || (countListFile < numberOfRegisterFile)){
					if((countListBase < numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						if(listFile.get(countListFile).getId() == listBase.get(countListBase).getId()){
							countListBase++;
							countListFile++;
						}else if(listFile.get(countListFile).getId() > listBase.get(countListBase).getId()){
							cityDAO.deleteById(session, listBase.get(countListBase).getId());
							statistic.incrementRegisterDeleted();
							countListBase++;
						}else{
							cityDAO.insert(session, listFile.get(countListFile));
							statistic.incrementRegisterWrite();
							countListFile++;
						}
					}else if((countListBase < numberOfRegisterBase) && (countListFile >= numberOfRegisterFile)){
						cityDAO.deleteById(session, listBase.get(countListBase).getId());
						statistic.incrementRegisterDeleted();
						countListBase++;
					}else if((countListBase >= numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						cityDAO.insert(session, listFile.get(countListFile));
						statistic.incrementRegisterWrite();
						countListFile++;
					}
				}
			}catch (EmptyTableException e){
				statistic = new StatisticFileUtil(new LoadCityEnvironment(), "loadCity(String filePath)", file, false, "");
				statistic.start();
				statistic.setNumberOfRegisterFile(listFile.size());
				for(int i = 0; i < listFile.size(); i++){
					try{
						statistic.incrementRegisterRead();
						cityDAO.insert(session, listFile.get(i));
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

	private ArrayList<CityEntity> loadFileIntoArray(File file) throws FileNotFoundException, FileEmptyException{
		ArrayList<CityEntity> listaCidade = new ArrayList<CityEntity>();
		Scanner scanner = new Scanner(file);
		StateDAO stateDAO = new StateDAO();
		CountryDAO countryDAO = new CountryDAO();
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		String var = "";
		String[] parms;
		if(!scanner.hasNext())
			throw new FileEmptyException("FILE (PATH: "+file.getPath()+" ) IS EMPTY");
		while(scanner.hasNext()){
			try{
				var = scanner.nextLine();
				parms = var.split(";");
				listaCidade.add(new CityEntity(Integer.valueOf(parms[0]), parms[1], stateDAO.selectByAcronym(session, parms[3]), countryDAO.selectByAcronym(session, parms[2])));
			}catch(Throwable e){
				e.printStackTrace();
			}
		}
		transaction.commit();
		session.close();
		scanner.close();
		return listaCidade;
	}
}