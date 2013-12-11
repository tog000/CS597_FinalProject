%% Import top 50.
filename = 'top50.csv';
delimiter = {'\t',','};
formatSpec = '%*s%*s%s%*s%*s%*s%*s%*s%*s%*s%*s%*s%f%*s%*s%[^\n\r]';
fileID = fopen(filename,'r');
dataArray = textscan(fileID, formatSpec, 'Delimiter', delimiter,  'ReturnOnError', false);
%dataArray2 = textscan(fileID, '%*s%*s%f%[^\n\r]', 'Delimiter', '\t',  'ReturnOnError', false);
fclose(fileID);
CRIME_NAMES = dataArray{:, 1};
CRIME_COUNT = dataArray{:, 2};
clearvars filename delimiter formatSpec fileID dataArray ans;

%% Import most correlated.
filename = 'sorted_output.txt';
delimiter = {'\t'};
formatSpec = '%d%s%f%*[^\n\r]';
fileID = fopen(filename,'r');
dataArray = textscan(fileID, formatSpec, 'Delimiter', delimiter,  'ReturnOnError', false);
fclose(fileID);
TOP_CORRELATED_NAME = dataArray{:, 2};
TOP_CORRELATED = dataArray{:, 3};
clearvars filename delimiter formatSpec fileID dataArray ans;

%% find indicator names

file_list = dir('scatter/*');
indicators = containers.Map;
for i=3:length(file_list)
    name = file_list(i).name;
    name = name(9:end-10);
    
    if name(end) == 'C'
        name = name(1:end-1);
    end
    if name(1) ~= 'W'
        indicators(name(2:end)) = 1;
    end
end

indicators = keys(indicators);

%%

all_examples = zeros(1,length(indicators));
all_classes = cell(1,1);

%crime_types={'BATTERY','SEX','ROBBERY','HOMICIDE','RECKLESS'};
%crime_types_explained={'BATTERY','SEXUAL ASSAULT','ROBBERY','HOMICIDE','RECKLESS CONDUCT'};
crime_types={'BATTERY','SEX','ROBBERY','HOMICIDE'};
crime_types_explained={'BATTERY','SEXUAL ASSAULT','ROBBERY','HOMICIDE'};
taken_crimes = [];

for ct=1:length(crime_types)
    
    crime_type_indices = zeros(length(CRIME_NAMES),length(crime_types));
    
    %sprintf('Doing it for crimes of type %s\n',crime_types{ct})
    
    for i=1:length(CRIME_NAMES)
        %sprintf('Searching for %s ON %s',crime_types{ct},CRIME_NAMES{i})
        if ~isempty(strfind(CRIME_NAMES{i},crime_types{ct}))
            %sprintf('FOUND!\n')
            crime_type_indices(i,1) = 1;
        end
    end
    
    crime_indices = find(crime_type_indices(:,1)==1);
    
    taken_crimes = [taken_crimes crime_indices'];
    
    figure(1);
    
    [h w] = size(all_examples);
    start_index = h;
    for i=1:length(crime_indices)
        for ind=1:length(indicators)
            
            if i == 1
                subplot(round(length(indicators)/4),4,ind);
            end
            
            file_list = dir(sprintf('scatter/*%s*C%d-*',indicators{ind},crime_indices(i)));
            if length(file_list) >= 1
                filename = ['scatter/' file_list(1).name];
                %disp(filename);
                data=load(filename);
                sorted = sortrows(data,-2);
               
                [h w] = size(data);
                if h > 20
                    if i==1
                        hold on;
                        plot([mean(sorted(1:4,3)) mean(sorted(1:4,3))],[0 10],'r','LineWidth',2)
                    end
                    all_examples(start_index+i,ind) = mean(sorted(1:4,3));
                    all_classes{start_index+i} = crime_types_explained{ct};
                end
            end
        end
    end
end

%indices_nan=isnan(all_examples);
sum2 = sum(all_examples(:,1:end),2);
%[h w] = size(all_examples);
all_examples = all_examples(sum2 ~= 0,:);


%%

indices = 1:length(CRIME_NAMES);
indices(taken_crimes) = [];
CRIME_NAMES{indices}

%%
column = 2;
plot(all_examples(find(all_classes=='BATTERY'),column), 'ro');
hold on;
plot(all_examples(find(all_classes=='SEX'),column), 'bo');
plot(all_examples(find(all_classes=='ROBBERY'),column), 'go');

%%

columns=[13 3 7];
examples = all_examples(:,columns);
classes = all_classes(2:end)';

%clear examples classes

%rnames = {'height','weight','spicy'};
%examples = [160 70 9; 150 80 10;150 56 10;180 100 7;210 100 3;200 80 1;160 50 4];
%classes = [1;1;1;1;2;2;2];

%t=classregtree(examples,classes,'method','classification');
tc = ClassificationTree.fit(examples,classes,'PredictorNames',indicators(columns),'minleaf',3,'minparent',2);
view(tc,'mode','graph') 

%%

figure(1);
for ind=1:length(indicators)
    
    subplot(round(length(indicators)/4),4,ind);
    
    dirn=sprintf('scatter/*%sC1-*',indicators{ind});
    file_list = dir(dirn);
    if length(file_list) >= 1
        filename = ['scatter/' file_list(1).name];
        %disp(filename);
        data=load(filename);
        hist(data(:,3),10);
        title(strrep(indicators{ind},'_',' '));
    end
end

