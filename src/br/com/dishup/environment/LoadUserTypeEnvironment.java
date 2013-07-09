package br.com.dishup.environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import org.hibernate.Session;
import org.hibernate.Transaction;
import br.com.dishup.core.entity.UserTypeEntity;
import br.com.dishup.core.exception.EmptyTableException;
import br.com.dishup.core.exception.FileEmptyException;
import br.com.dishup.core.persistence.HibernateUtil;
import br.com.dishup.core.persistence.UserTypeDAO;
import br.com.dishup.core.util.StatisticFileUtil;
import br.com.dishup.core.util.UserTypeComparatorUtil;

public class LoadUserTypeEnvironment {

	public void loadUserType(String filePath){
		File file = new File(filePath);
		StatisticFileUtil statistic = new StatisticFileUtil(new LoadUserTypeEnvironment(), "loadUserType(String filePath)", file, true, "dishup.tipo_usuario");
		statistic.start();
		ArrayList<UserTypeEntity> listFile = new ArrayList<UserTypeEntity>();
		UserTypeDAO userTypeDAO = new UserTypeDAO();
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		try {
			listFile = loadFileIntoArray(file);
			Collections.sort(listFile, new UserTypeComparatorUtil());
			statistic.setNumberOfRegisterFile(listFile.size());
			List<UserTypeEntity> listBase = new ArrayList<UserTypeEntity>();
			try{
				listBase = userTypeDAO.selectAllOrderById(session);
				statistic.setNumberOfRegisterBase(listBase.size());
				int countListBase = 0, countListFile = 0, numberOfRegisterBase = listBase.size(), numberOfRegisterFile = listFile.size();
				while((countListBase < numberOfRegisterBase) || (countListFile < numberOfRegisterFile)){
					if((countListBase < numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						if(listFile.get(countListFile).getId() == listBase.get(countListBase).getId()){
							countListBase++;
							countListFile++;
						}else if(listFile.get(countListFile).getId() > listBase.get(countListBase).getId()){
							userTypeDAO.deleteById(session,listBase.get(countListBase).getId());
							statistic.incrementRegisterDeleted();
							countListBase++;
						}else{
							userTypeDAO.insert(session,listFile.get(countListFile));
							statistic.incrementRegisterWrite();
							countListFile++;
						}
					}else if((countListBase < numberOfRegisterBase) && (countListFile >= numberOfRegisterFile)){
						userTypeDAO.deleteById(session,listBase.get(countListBase).getId());
						statistic.incrementRegisterDeleted();
						countListBase++;
					}else if((countListBase >= numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						userTypeDAO.insert(session,listFile.get(countListFile));
						statistic.incrementRegisterWrite();
						countListFile++;
					}
				}
			}catch(EmptyTableException e){
				statistic = new StatisticFileUtil(new LoadUserTypeEnvironment(), "loadUserType(String filePath)", file, false, "");
				statistic.start();
				statistic.setNumberOfRegisterFile(listFile.size());
				for(int i = 0; i < listFile.size(); i++){
					try{
						statistic.incrementRegisterRead();
						userTypeDAO.insert(session,listFile.get(i));
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
	
	private ArrayList<UserTypeEntity> loadFileIntoArray(File file) throws FileNotFoundException, FileEmptyException{
		ArrayList<UserTypeEntity> list = new ArrayList<UserTypeEntity>();
		Scanner scanner = new Scanner(file);
		String var = "";
		String[] parms;
		if(!scanner.hasNext())
			throw new FileEmptyException("FILE (PATH: "+file.getPath()+" ) IS EMPTY");
		while(scanner.hasNext()){
			try{
				var = scanner.nextLine();
				parms = var.split(";");
				list.add(new UserTypeEntity(Integer.valueOf(parms[0]), parms[1], parms[2]));
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		scanner.close();
		return list;
	}
}