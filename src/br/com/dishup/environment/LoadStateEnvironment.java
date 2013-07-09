package br.com.dishup.environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import org.hibernate.Session;
import org.hibernate.Transaction;
import br.com.dishup.core.entity.StateEntity;
import br.com.dishup.core.exception.EmptyTableException;
import br.com.dishup.core.exception.FileEmptyException;
import br.com.dishup.core.exception.CountryNotFoundException;
import br.com.dishup.core.persistence.StateDAO;
import br.com.dishup.core.persistence.HibernateUtil;
import br.com.dishup.core.persistence.CountryDAO;
import br.com.dishup.core.util.StateComparatorUtil;
import br.com.dishup.core.util.StatisticFileUtil;

public class LoadStateEnvironment {
	
	public void loadState(String filePath){
		File file = new File(filePath);
		StatisticFileUtil statistic = new StatisticFileUtil(new LoadStateEnvironment(), "loadState(String filePath)", file, true, "dishup.estado");
		statistic.start();
		ArrayList<StateEntity> listFile = new ArrayList<StateEntity>();
		StateDAO stateDAO = new StateDAO();
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		
		try{
			listFile = loadFileIntoArray(file);
			Collections.sort(listFile, new StateComparatorUtil());
			statistic.setNumberOfRegisterFile(listFile.size());
			List<StateEntity> listBase = new ArrayList<StateEntity>();;
			try {
				listBase = stateDAO.selectAllOrderById(session);
				statistic.setNumberOfRegisterBase(listBase.size());
				int countListBase = 0, countListFile = 0, numberOfRegisterBase = listBase.size(), numberOfRegisterFile = listFile.size();
				while((countListBase < numberOfRegisterBase) || (countListFile < numberOfRegisterFile)){
					if((countListBase < numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						if(listFile.get(countListFile).getId() == listBase.get(countListBase).getId()){
							countListBase++;
							countListFile++;
						}else if(listFile.get(countListFile).getId() > listBase.get(countListBase).getId()){
							stateDAO.deleteById(session, listBase.get(countListBase).getId());
							statistic.incrementRegisterDeleted();
							countListBase++;
						}else{
							stateDAO.insert(session, listFile.get(countListFile));
							statistic.incrementRegisterWrite();
							countListFile++;
						}
					}else if((countListBase < numberOfRegisterBase) && (countListFile >= numberOfRegisterFile)){
						stateDAO.deleteById(session, listBase.get(countListBase).getId());
						statistic.incrementRegisterDeleted();
						countListBase++;
					}else if((countListBase >= numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						stateDAO.insert(session, listFile.get(countListFile));
						statistic.incrementRegisterWrite();
						countListFile++;
					}
				}
			}catch (EmptyTableException e){
				statistic = new StatisticFileUtil(new LoadStateEnvironment(), "loadState(String filePath)", file, false, "");
				statistic.start();
				statistic.setNumberOfRegisterFile(listFile.size());
				for(int i = 0; i < listFile.size(); i++){
					try{
						statistic.incrementRegisterRead();
						stateDAO.insert(session, listFile.get(i));
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
	
	private ArrayList<StateEntity> loadFileIntoArray(File file) throws FileNotFoundException, FileEmptyException, CountryNotFoundException {
		ArrayList<StateEntity> listaEstado = new ArrayList<StateEntity>();
		CountryDAO countryDAO = new CountryDAO();
		Scanner scanner = new Scanner(file);
		String var = "";
		String[] parms;
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		if(!scanner.hasNext())
			throw new FileEmptyException("FILE (PATH: "+file.getPath()+" ) IS EMPTY");
		while(scanner.hasNext()){
			try{
				var = scanner.nextLine();
				parms = var.split(";");
				listaEstado.add(new StateEntity(Integer.valueOf(parms[0]), parms[1], parms[2], countryDAO.selectByAcronym(session, parms[3])));
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		transaction.commit();
		session.close();
		scanner.close();
		return listaEstado;
	}
}