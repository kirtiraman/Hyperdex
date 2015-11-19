import java.util.ArrayList;
import java.util.HashMap;

import org.hyperdex.client.Iterator;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.hyperdex.client.Client;
import org.hyperdex.client.HyperDexClientException;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class HyperDexClient extends DB{
    public static String ip = "";
    public static int port = 0;
    public Properties properties;
    public Client c = null;

    public boolean init() throws DBException {
        properties = getProperties();
        ip = properties.getProperty("hostip");
        port = Integer.parseInt(properties.getProperty("dbport"));
        c = new Client(ip, port);
        System.out.println("Successful Connection");
        return true;
    }


    @Override
    public int insertEntity(String entitySet, String entityPK,
            HashMap<String, ByteIterator> values, boolean insertImage) {
           
            String val;
            if (entitySet.equalsIgnoreCase("users")) {
                Map<String, Object> userMap = new HashMap<String, Object>();
                for (String k : values.keySet()) {
                    if (k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic")) continue;
                    val = new String(values.get(k).toString());
                    userMap.put(k, val);
                }
                ArrayList<String> conf_friend = new ArrayList<String>();
                ArrayList<String> pend_friend = new ArrayList<String>();
                userMap.put("confFriend", conf_friend);
                userMap.put("pendFriend", pend_friend);
                try {
                    c.put(entitySet, entityPK, userMap);
                }
                catch (HyperDexClientException e) {
                    System.out.println("HyperDex Client Exception: " + e);
                }
            }
            if (entitySet.equalsIgnoreCase("resources")) {
                Map<String, Object> recsMap = new HashMap<String, Object>();
                for (String k : values.keySet()) {
                    val = new String(values.get(k).toString());
                    recsMap.put(k, val);
                }
                try {
                    c.put(entitySet, entityPK, recsMap);
                }
                catch (HyperDexClientException e) {
                    System.out.println("HyperDex Client Exception: " + e);
                }
            }
            if (entitySet.equalsIgnoreCase("manipulations")) {
                Map<String, Object> manipulationMap = new HashMap<String, Object>();
                for (String k : values.keySet()) {
                    val = new String(values.get(k).toString());
                    if(!k.equals("timestamp")) {
                        manipulationMap.put(k, val);
                    } else {
                        manipulationMap.put("time_stamp", val);
                    }
                       
                }
                try {
                    c.put(entitySet, entityPK, manipulationMap);
                }
                catch (HyperDexClientException e) {
                    System.out.println("HyperDex Client Exception: " + e);
                }
            }
        return 0;
    }

    @Override
    public int viewProfile(int requesterID, int profileOwnerID,
            HashMap<String, ByteIterator> result, boolean insertImage,
            boolean testMode) {
         if (requesterID < 0 || profileOwnerID < 0) {
                return -1;
            }
            try {
                Map<String, Object> profileOwner = c.get("users", Integer.toString(profileOwnerID));
                System.out.println(profileOwner);
                ArrayList<String> pdf = (ArrayList<String>) profileOwner.get("pendFriend");
                ArrayList<String> cdf = (ArrayList<String>) profileOwner.get("confFriend");
                result.put("friendcount", new ObjectByteIterator(Integer.toString(cdf.size()).getBytes()));
                if (requesterID == profileOwnerID) {
                    result.put("pendingcount", new ObjectByteIterator(Integer.toString(pdf.size()).getBytes()));
                }
                HashMap<String, Object> predicate = new HashMap<String, Object>();
                predicate.put("walluserid", Integer.toString(profileOwnerID));
                Long count = c.count("resources", predicate);
                result.put("resourcecount", new ObjectByteIterator(Long.toString(count).getBytes()));
            }
            catch (HyperDexClientException e) {
                e.printStackTrace();
            }
        return 0;
    }

    @Override
    public int listFriends(int requesterID, int profileOwnerID,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
            boolean insertImage, boolean testMode) {
        if(requesterID<1 || profileOwnerID<1)
        {
            return -1;
        }
         try
        {
            Map<String, Object> profileOwner = c.get("users", Integer.toString(profileOwnerID));
            if (profileOwner == null) {
                return 0;
            } else {
                ArrayList<String> cdf = (ArrayList<String>) profileOwner
                        .get("confFriend");

                if (cdf.size() == 0) {
                    return 0;
                }
                if (fields == null || fields.size() == 0) {
                    fields = profileOwner.keySet();
                    fields.remove("confFriend");
                    fields.remove("pendFriend");
                }

                for (int k = 0; k < cdf.size(); k++) {
                    Map<String, Object> f1 = c.get("users", cdf.get(k));
                    HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();

                    for (String field : fields) {

                        map.put(field, new ObjectByteIterator(f1.get(field)
                                .toString().getBytes()));

                    }
                    result.add(map);

                }
            }
            } catch (HyperDexClientException e) {
                System.out.println("Hyperdex Client Exception: " + e);
                return -1;
        }
        //System.out.println("NOTICE listFriends ok with " + profileOwnerID);
        return 0;
    }

    @Override
    public int viewFriendReq(int profileOwnerID,
            Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
            boolean testMode) {
        if(profileOwnerID<0)
        {
            return -1;
        }
        
         try
        {
            Map<String, Object> profileOwner = c.get("users", Integer.toString(profileOwnerID));
            ArrayList<String> pdf = (ArrayList<String>) profileOwner.get("pendFriend");
            ArrayList<String> cdf = (ArrayList<String>) profileOwner.get("conFriend");
          
           
         /*   if(pdf.size()==0)
            {
                return 0;
            }*/


            for (int k=0;k<pdf.size();k++) {
                HashMap<String,ByteIterator> map = new HashMap<String,ByteIterator>();
                Map<String,Object> f1 = c.get("users",pdf.get(k));
                f1.remove("pendFriend");
                f1.remove("confFriend");
               
                for (String field : f1.keySet()) {
                   
                    map.put(field,new ObjectByteIterator(f1.get(field).toString().getBytes()));
                   
                }
                results.add(map);

               
            }

            } catch (HyperDexClientException e) {
                System.out.println("Hyperdex Client Exception: " + e);
                return -1;
           
        }
        System.out.println("NOTICE viewFriendReq ok with " + profileOwnerID);
       
        return 0;
    }

    @Override
    public int acceptFriend(int inviterID, int inviteeID) {
        try
        {
        Map<String, Object> profileOwner = c.get("users", Integer.toString(inviteeID));
        ArrayList<String> pdf = (ArrayList<String>) profileOwner.get("pendFriend");
        ArrayList<String> cdf = (ArrayList<String>) profileOwner.get("confFriend");
        pdf.remove(Integer.toString(inviterID));
        cdf.add(Integer.toString(inviterID));
        }
        catch (HyperDexClientException e) {
            System.out.println("Hyperdex Client Exception: " + e);
            return -1;
        }
        int res = CreateFriendship(inviterID, inviteeID);
        if (res != 0) {
            System.out.println("acceptFriend CreateFriendship failed");
            return -1;
        }

        System.out.println("NOTICE acceptFriend ok with " + inviteeID + " and " + inviterID);
       
        return 0;
    }

    @Override
    public int rejectFriend(int inviterID, int inviteeID) {
        if(inviterID < 0 || inviteeID < 0) {
            return -1;
        }
        try
        {
        Map<String, Object> profileOwner = c.get("users", Integer.toString(inviteeID));
        ArrayList<String> pdf = (ArrayList<String>) profileOwner.get("pendFriend");
        pdf.remove(Integer.toString(inviterID));
        }
        catch (HyperDexClientException e) {
            System.out.println("Hyperdex Client Exception: " + e);
            return -1;
        }
       
        return 0;
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID) {
        if (inviterID < 0 || inviteeID < 0) {
            return -1;
        }
        try {
            Map<String, Object> user2 = c.get("users", Integer.toString(inviteeID));
            ArrayList<String> pdf = (ArrayList<String>) user2.get("pendFriend");
            pdf.add(Integer.toString(inviterID));
            user2.put("pendFriend", pdf);
            c.put("users", Integer.toString(inviteeID), user2);
        }
        catch (HyperDexClientException e) {
            System.out.println("Hyperdex Client Exception: " + e);
        }
        System.out.println("Invite Successful");
       
        return 0;
    }

    @Override
    public int viewTopKResources(int requesterID, int profileOwnerID, int k,
            Vector<HashMap<String, ByteIterator>> result) {
        Map<String, Object> predicate = new HashMap<String, Object>();
        predicate.put("walluserid", Integer.toString(profileOwnerID));
        Iterator itr =  c.sorted_search("resources", predicate, "rid", k, true);
           
        try {
            while (itr.hasNext()) {
                Map<String, Object> r1 = (Map<String, Object>) itr.next();
                HashMap<String, ByteIterator> rec1 = new HashMap<String, ByteIterator>();
                rec1.put("rid", new ObjectByteIterator(r1.get("rid").toString().getBytes()));
                rec1.put("walluserid", new ObjectByteIterator(r1.get("walluserid").toString().getBytes()));
                result.add(rec1);
            }
        } catch (HyperDexClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        return 0;
    }

    @Override
    public int getCreatedResources(int creatorID,
            Vector<HashMap<String, ByteIterator>> result) {
        Map<String, Object> predicate = new HashMap<String, Object>();
        predicate.put("creatorid", Integer.toString(creatorID));
        Iterator itr =  c.search("resources", predicate);
           
        try {
            while (itr.hasNext()) {
                Map<String, Object> r1 = (Map<String, Object>) itr.next();
                HashMap<String, ByteIterator> rec1 = new HashMap<String, ByteIterator>();
                rec1.put("rid", new ObjectByteIterator(r1.get("rid").toString().getBytes()));
                rec1.put("creatorid", new ObjectByteIterator(r1.get("creatorid").toString().getBytes()));
                result.add(rec1);
            }
        } catch (HyperDexClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public int viewCommentOnResource(int requesterID, int profileOwnerID,
            int resourceID, Vector<HashMap<String, ByteIterator>> result) {
        Map<String, Object> predicate = new HashMap<String, Object>();
        predicate.put("rid", Integer.toString(resourceID));
       
        Iterator itr =  c.search("manipulations", predicate);
        try {
            while(itr.hasNext()){
                Map<String, Object> c1 = (Map<String, Object>) itr.next();
               
                if(c1.get("type").equals("comment")) {
                    HashMap<String, ByteIterator> com1 = new HashMap<String, ByteIterator>();
                    com1.put("rid", new ObjectByteIterator(c1.get("content").toString().getBytes()));
                    result.add(com1);
                }
            }
        } catch (HyperDexClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
       
        return 0;
    }

    @Override
    public int postCommentOnResource(int commentCreatorID,
            int resourceCreatorID, int resourceID,
            HashMap<String, ByteIterator> values) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (String k : values.keySet()) {
            if(!k.equals("mid")) {
                if(!k.equals("timestamp")){
                    map.put(k, values.get(k).toString());
                } else {
                    map.put("time_stamp", values.get(k).toString());
                }
               
            }
        }
       
        map.put("creatorid", Integer.toString(resourceCreatorID));
        map.put("rid", Integer.toString(resourceID));
        map.put("modifierid", Integer.toString(commentCreatorID));
       
        try {
            c.put("manipulations", values.get("mid").toString(), map);
        } catch (HyperDexClientException e) {
            System.out.println("HyperDex Client Exception in postommentOnResource " + e);
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    @Override
    public int delCommentOnResource(int resourceCreatorID, int resourceID,
            int manipulationID) {
        try {
            c.del("manipulations", Integer.toString(manipulationID));
        } catch (HyperDexClientException e) {
            System.out.println("HyperDex Client Exception in delCommentOnResource " + e);
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public int thawFriendship(int friendid1, int friendid2) {
        try {
            Map<String, Object> profileOwner = c.get("users",
                    Integer.toString(friendid1));
            ArrayList<String> pdf = (ArrayList<String>) profileOwner
                    .get("pendFriend");
            int index = pdf.indexOf(Integer.toString(friendid1));
            if (index == -1) {
                System.out.println("ERROR acceptFriend error , invitee "
                        + friendid1 + " doesnot have inviter " + friendid1
                        + " as pending friend.");
            } else {
                pdf.remove(index);
            }
            Map<String, Object> profileOwner2 = c.get("users",
                    Integer.toString(friendid2));
            ArrayList<String> pdf2 = (ArrayList<String>) profileOwner
                    .get("pendFriend");
            int index2 = pdf.indexOf(Integer.toString(friendid1));
            if (index == -1) {
                System.out.println("ERROR acceptFriend error , invitee "
                        + friendid2 + " doesnot have inviter " + friendid2
                        + " as pending friend.");
            } else {
                pdf.remove(index);
            }
        } catch (HyperDexClientException e) {
            System.out.println("Hyperdex Client Exception: " + e);
            return -1;
        }
        return 0;
    }

    @Override
    public HashMap<String, String> getInitialStats() {
         HashMap<String, String> initStats = new HashMap<String, String>();
            initStats.put("usercount", "10000");
            initStats.put("resourcesperuser", "100");
            initStats.put("avgfriendsperuser", "100");
            initStats.put("avgpendingperuser", "0");
            return initStats;
    }

    @Override
    public int CreateFriendship(int friendid1, int friendid2) {

        if (friendid1 < 0 || friendid2 < 0) {
            return -1;
        }
        try {
            Map<String, Object> user1 = this.c.get("users",
                    (Object) Integer.toString(friendid1));
            Map<String, Object> user2 = this.c.get("users",
                    (Object) Integer.toString(friendid2));
            ArrayList<String> conffriend1 = (ArrayList<String>) user1
                    .get("confFriend");
            if (conffriend1.contains(Integer.toString(friendid2))) {
                System.out.println("Error: friend already exist, friend1: "
                        + friendid1 + " friend2: " + friendid2);
                return -1;
            }

            conffriend1.add(Integer.toString(friendid2));
            user1.put("confFriend", conffriend1);
            c.put("users", Integer.toString(friendid1), user1);
           
            ArrayList<String> conffriend2 = (ArrayList<String>) user2.get("confFriend");
            if (conffriend2.contains(Integer.toString(friendid1))) {
                System.out.println("Error: friend already exist, friend1: "
                        + friendid1 + " friend2: " + friendid2);
                return -1;
            }
            conffriend2.add(Integer.toString(friendid1));
            user2.put("confFriend", conffriend2);
            c.put("users", Integer.toString(friendid2), user2);

        } catch (HyperDexClientException e) {
            System.out.println("Hyperdex Client Exception: " + e);
            return -1;
        }

        return 0;
    }

    @Override
    public void createSchema(Properties props) {
        // TODO Auto-generated method stub
       
    }

    @Override
    public int queryPendingFriendshipIds(int memberID,
            Vector<Integer> pendingIds) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int queryConfirmedFriendshipIds(int memberID,
            Vector<Integer> confirmedIds) {
        // TODO Auto-generated method stub
        return 0;
    }

}



